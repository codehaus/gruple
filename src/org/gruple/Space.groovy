/*
 *    Copyright 2009 Vanessa Williams
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*
 * Space.groovy
 * Created on Apr 4, 2009
 */

package org.gruple

import groovy.time.TimeDuration
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Class representing a tuplespace. Tuples {@Link Tuple} can be inserted, read, and
 * removed using put(), get(), and take() respectively.
 *
 * Fields in a tuple are named, but not ordered.
 * Only immutable objects may be added to a Tuple. This includes the
 * following types:
 * <ul>
 * <li>Integer</li>
 * <li>Short</li>
 * <li>Long</li>
 * <li>Byte</li>
 * <li>Float</li>
 * <li>Double</li>
 * <li>BigInteger</li>
 * <li>BigDecimal</li>
 * <li>Boolean</li>
 * <li>Character</li>
 * <li>String</li>
 * <li>URI</li>
 * <li>Date (we assume no deprecated methods are ever used)</li>
 * <li>Enum types</li>
 * <li>Classes using the @Immutable annotation</li>
 * <li>Collections and Maps of any of the above</li>
 * </ul>
 * Note that Arrays are not supported as Tuple fields because they can't be made immutable.
 * 
 * @see Tuple
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class Space {

    // TODO: implement notify
    
    private static final Logger LOG = Logger.getLogger(Space.name)

    /**
     * Convenience constant to indicate immediate return from get() and take()
     * operations.
     */
    public static final Long NO_WAIT = 0

    /**
     * Convenience constant to indicate indefinite wait for get() and take()
     * operations. Note that this is the default if no timeout is supplied, so
     * the constant is necessary only for clarity.
     */
    public static final Long WAIT_FOREVER = -1

    /**
     * Convenience constant to indicate indefinite ttl for put tuples.
     * This is the default value.
     */
    public static final Long FOREVER = -1

    /**
    * The space name. Required to look up a reference to a particular space.
    */
    final String name

    // TODO: use factory to get appropriate implementation of TupleStore
    /*
     * Main store for tuples and templates
     */
    private TupleStore store = new MemoryTupleStore()

    /*
     * Stores used to handle transactions
     */
    private Map workingStores = new ConcurrentHashMap()  // holds tuples extracted by transactions
    private Map rollbackStores = new ConcurrentHashMap() // holds tuples added by transactions

    /*
     * List of transactions in which this space is enrolled
     */
    private List transactions = Collections.synchronizedList(new ArrayList())

    private volatile boolean shuttingDown = false

    private final Timer expiryTimer

    /*
     * This readWrite lock is used a little unusually. A read lock is used for all operations
     * except close, commit, and rollback. For those operations, a writeLock is acquired so
     * that no other operations will be concurrent.
     */
    private ReentrantReadWriteLock spaceLock = new ReentrantReadWriteLock()

    /**
     * Creates a new instance of TupleSpace.
     *
     * Not called by applications, use SpaceService.getSpace(name) instead.
     *
     */
    protected Space(String name) {
        this.name = name
        expiryTimer = new Timer("Expired Tuple Reaper", true) // is a daemon
    }

    /**
     * Insert a map as a tuple into the space.
     *
     * If the map provided does not conform to the requirements for
     * tuples {@Link Tuple} (immutable value types only, string keys only)
     * an IllegalArgumentException will be thrown.
     *
     * @see Tuple
     * @param a map of tuple fields
     * @param the tuple's time-to-live in milliseconds. -1(=FOREVER)
     *        is the default
     * @param Transaction, default is null
     */
    void put(Map tupleFields, Long ttl=FOREVER, Transaction txn=null ) {
        put(new Tuple(tupleFields), ttl, txn)
    }

    /**
     * Convenience function allowing the use of TimeCategory for the ttl
     * parameter.
     * e.g.
     * <code>
     * use(TimeCategory) {
     *     put([field1:1, field2:2], 2.minutes)
     * }
     * </code>
     * @param a map of {@Link Tuple} fields
     * @param a Duration for the tuple's time-to-live
     * @param Transaction, default is null
     */
    void put(Map tupleFields, TimeDuration ttl, Transaction txn=null) {
        put(tupleFields, ttl.toMilliseconds(), txn)
    }

    /**
     * Override leftShift to "append" a map (as Tuple) to the space with default ttl
     * @param the map of {@Link Tuple} fields to insert
     */
    void leftShift(Map tupleFields) {
        put(tupleFields)
    }

    /**
     * Consume a tuple which matches the template. Templates use null as
     * wildcard field values.
     *
     * @param a Map of template fields (must adhere to rules for {@Link Tuple})
     * @param timeout in milliseconds; 0 returns immediately; default is
     *          -1(=WAIT_FOREVER)
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map take(Map templateFields, Long timeout=WAIT_FOREVER, Transaction txn=null) throws InterruptedException {

        if (templateFields == null)
            throw new IllegalArgumentException("Field map may not be null.")

        Tuple match = take(new Tuple(templateFields), timeout, txn)
        return makeTupleIntoMap(match)
    }

    /**
     * Convenience function allowing the use of TimeCategory for the timeout
     * parameter.
     * e.g.
     * <code>
     * use(TimeCategory) {
     *     take([field1:1, field2:null], 2.minutes)
     * }
     * </code>
     * @param a map of fields
     * @param a Duration for the tuple's time-to-live
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map take(Map templateFields, TimeDuration timeout, Transaction txn=null) throws InterruptedException {
        return take(templateFields, timeout.toMilliseconds(), txn)
    }

    /**
     * Non-destructive read of a tuple which matches the template. Templates
     * use null as wildcard field values.
     *
     * @param a Map of template fields (must adhere to rules for {@Link Tuple})
     * @param timeout in milliseconds; 0 returns immediately; default is
     *        -1(=WAIT_FOREVER)
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map get(Map templateFields, Long timeout=WAIT_FOREVER, Transaction txn=null) throws InterruptedException {

        if (templateFields == null)
            throw new IllegalArgumentException("Field map may not be null.")

        Tuple match = get(new Tuple(templateFields), timeout, null)
        return makeTupleIntoMap(match)
    }

    /**
     * Convenience function allowing the use of TimeCategory for the timeout
     * parameter.
     * e.g.
     * <code>
     * use(TimeCategory) {
     *     get([field1:1, field2:null], 2.minutes)
     * }
     * </code>
     * @param a map of tuple fields
     * @param a Duration for the tuple's time-to-live
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map get(Map templateFields, TimeDuration timeout, Transaction txn=null) throws InterruptedException {
        return get(templateFields, timeout.toMilliseconds(), txn)
    }

    /**
     * Insert a tuple into the space. Applications should use the put(Map, Long) and put(Map, Duration versions).
     *
     * @param the tuple to insert. May not be null.
     * @param the tuple's time-to-live in milliseconds. -1(=FOREVER)
     *        is the default
     * @param Transaction, default is null
     */
    protected void put(Tuple tuple, Long ttl=FOREVER, Transaction txn=null) {

        if (!tuple)
            throw new IllegalArgumentException("Attempt to put a null tuple")

        if (tuple.hasFormals())
            throw new IllegalArgumentException("Can't put a template/anti-tuple into the space")

        if (shuttingDown) return

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Putting tuple $tuple".toString())

//        spaceLock.readLock().lockInterruptibly()
//        try {
            // make sure the transaction knows about this space's participation
            List templates
            if (txn != null) {
                enroll(txn)
                TupleStore rollbackStore = rollbackStores[txn]
                // store the tuple in a temporary rollback store
                rollbackStore.storeTuple(tuple)

                if (ttl != FOREVER) expiryTimer.runAfter(ttl as int) {
                    store.removeTuple(tuple)        // the ttl could expire after transaction has committed
                    rollbackStore.removeTuple(tuple)// so delete from both possible stores
                }

                templates = rollbackStore.getWaitingTemplates(tuple)

//                Template lastTemplate = templates.isEmpty() ? null : templates.last()
//                if (lastTemplate != null && !lastTemplate.destructive) {
//                    // we could notify more waiting threads
//                    // search the main store and other transactions for other possible candidates
//                    templates.add(store.getWaitingTemplates(tuple))
//                    lastTemplate = templates.isEmpty() ? null : templates.last()
//                    if (lastTemplate != null && !lastTemplate.destructive) {
//                        // searching the main store still leaves us with the possibility of
//                        // notifying more threads. Search all transactional rollback stores
//                        for (entry in rollbackStores) {
//                            TupleStore rStore = entry.value
//                            templates.add(rStore.getWaitingTemplates(tuple))
//                            lastTemplate = templates.isEmpty() ? null : templates.last()
//                            if (lastTemplate != null && lastTemplate.destructive) break
//                        }
//                    }
//                }
            }
            else {
                store.storeTuple(tuple)

                if (ttl != FOREVER) expiryTimer.runAfter(ttl as int) {
                    store.removeTuple(tuple)
                }

                // search the template map. If a match is found, notify() the
                // waiting thread(s).
                templates = store.getWaitingTemplates(tuple)
            }

            for (Template template in templates) {
                synchronized(template) {
                    template.notify()
                }
            }
//        }
//        finally {
//            spaceLock.readLock().unlock()
//        }



    }

    /**
     * Consume a tuple which matches the template. Templates use null as
     * wildcard field values.
     *
     * Applications should use the take(Map, Long) and take(Map, Duration) versions.
     *
     * @param a template to match.
     * @param timeout time to block for; 0 returns immediately,
     *        -1(=WAIT_FOREVER) is the default. Other values are
     *        in milliseconds.
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return found tuple, or null if none found within timeout
     */
    protected Tuple take(Tuple antiTuple, Long timeout=WAIT_FOREVER, Transaction txn=null) throws InterruptedException {

        if (!antiTuple)
            throw new IllegalArgumentException("Template may not be null.")

        if (shuttingDown) return null

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Taking with template $antiTuple and timeout=$timeout")

        Tuple match
        Template template = new Template(antiTuple, true)
//        spaceLock.readLock().lockInterruptibly()
//        try {
            
            long start = System.currentTimeMillis()
            long timeToWait = timeout

            TupleStore rollbackStore
            if (txn != null) {
                // make sure we're part of the transaction
                enroll(txn)
                // store the template in a temporary store
                rollbackStore = rollbackStores[txn]
                rollbackStore.storeTemplate(template)
            }
            else {
                store.storeTemplate(template)
            }

            synchronized (template) {
                while (!shuttingDown) {
                    if (txn != null) {
                        // check the temporary store for a match 1st
                        match = rollbackStore.getMatch(template, true)
                        if (match) break
                    }
                    // check the main store
                    match = store.getMatch(template, true)
                    if (match) break
                    // wait, if necessary
                    if (timeToWait <= 0 && timeout != Space.WAIT_FOREVER) break
                    timeToWait = doWait(template, timeout, timeToWait, start)
                }
            }

            if (match && txn != null) {
                // if a match is found, save it in a working store so it can still be read by other threads
                // and can be returned to the main store if the transaction is aborted/rolledback
                TupleStore workingStore = workingStores[txn]
                workingStore.storeTuple(match)
            }
            else {
                // cleanup templates not removed by getMatch
                if (txn != null) rollbackStore.removeTemplate(template)
                else store.removeTemplate(template)
            }
//        }
//        finally {
//            spaceLock.readLock().unlock()
//        }
        return match

    }

    /**
     * Non-destructive read of a tuple which matches the template. Templates
     * use null as wildcard field values.
     *
     * Applications should use the get(Map, Long) and get(Map, Duration) versions.
     *
     * @param template to match
     * @param timeout time to block for; 0 returns immediately,
     *        -1(=WAIT_FOREVER) is the default. Other values are
     *        in milliseconds
     * @param Transaction, default is null
     * @throws InterruptedException (for the sake of Java clients)
     * @return the matched Tuple or null if none found
     */
    protected Tuple get(Tuple antiTuple, Long timeout=WAIT_FOREVER, Transaction txn=null) throws InterruptedException {

        if (!antiTuple)
            throw new IllegalArgumentException("Template may not be null.")

        if (shuttingDown) return null

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Getting with template $antiTuple and timeout=$timeout")

        Tuple match
//        spaceLock.readLock().lockInterruptibly()
//        try {
            
            Template template = new Template(antiTuple, false)

            long start = System.currentTimeMillis()
            long timeToWait = timeout
            TupleStore rollbackStore
            if (txn != null) {
                // make sure we're enrolled in the transaction
                enroll(txn)
                // store the template in a temporary store
                rollbackStore = rollbackStores[txn]
                rollbackStore.storeTemplate(template)
            }
            else {
                store.storeTemplate(template)
            }

            synchronized (template) {
                while (!shuttingDown) {
                    if (txn != null) {
                        // check the temporary store for a match 1st
                        match = rollbackStore.getMatch(template, false)
                        if (match) break
                    }
                    // if none found, check the main store
                    match = store.getMatch(template, false)
                    if (match) break
                    // if still none found, check working stores of all transactions
                    match = searchWorkingStores(template, txn)
                    if (match) break
                    // wait, if necessary
                    if (timeToWait <= 0 && timeout != Space.WAIT_FOREVER) break
                    timeToWait = doWait(template, timeout, timeToWait, start)
                }
            }

            // clean up templates
            if (!match) {
                if (txn != null) {
                    rollbackStore.removeTemplate(template)
                }
                else {
                    store.removeTemplate(template)
                }
            }
//        }
//        finally {
//            // clean up the template we stored, in case getMatch couldn't do it for us
//            spaceLock.readLock().unlock()
//        }
        return match

    }

    /**
     * Close a space. Clears out all data structures, and notifies all
     * waiting threads, so use with caution. Not called directly by
     * applications. Use SpaceService.close(String name) instead.
     */
    protected synchronized close() {

        if (!shuttingDown) {
            shuttingDown = true
            store.deleteStorage();
        }


    }

    protected enroll(Transaction txn) {
        if (this.transactions.contains(txn)) return // alreday enrolled
        // tell the transaction this space is involved in the transaction
        txn.enrollSpace(this)
        // remember we're a part of this transaction
        this.transactions.add(txn)
        // prepare the working and rollback stores
        // TODO: use factory to get appropriate implementation of TupleStore
        this.rollbackStores[txn] = new MemoryTupleStore()
        this.workingStores[txn] = new MemoryTupleStore()
    }

    /**
     * Commit the transaction for this space. Should not be called
     * by applications. Use Transaction.commit() instead.
     *
     * @see org.gruple.Transaction
     */
    protected synchronized commit(Transaction txn) {

//        spaceLock.writeLock().lockInterruptibly()
//        try{
            // insert tuples from rollback store into main store
            TupleStore rStore = rollbackStores[txn]
            List rTuples = rStore.allTuples
            for (tuple in rTuples) {
//                store.storeTuple(tuple)
                put(tuple)
            }

            // remove tuples in working store from main store
            TupleStore wStore = workingStores[txn]
            List wTuples = wStore.allTuples
            for (tuple in wTuples) {
                store.removeTuple(tuple)
            }

            // discard the transaction and its stores
            transactions.remove(txn)
            rollbackStores.remove(txn)
            rStore.deleteStorage()
            workingStores.remove(txn)
            wStore.deleteStorage()
//        } finally {
//            spaceLock.writeLock().unlock()
//        }
    }

    /**
     * Rollback the transaction for this space. Should not be called
     * by applications. Use Transaction.rollback() instead.
     *
     * @see org.gruple.Transaction
     */
    protected synchronized rollback(Transaction txn) {

//        spaceLock.writeLock().lockInterruptibly()
//
//        try {
            // return tuples in working store to main store
            TupleStore wStore = workingStores[txn]
            List wTuples = wStore.allTuples
            for (tuple in wTuples) {
//                store.storeTuple(tuple)
                put tuple
            }

            transactions.remove(txn)
            TupleStore rStore = rollbackStores[txn]
            rollbackStores.remove(txn)
            rStore.deleteStorage()
            workingStores.remove(txn)
            wStore.deleteStorage()
//        } finally {
//            spaceLock.writeLock().unlock()
//        }
    }
    
    private Map makeTupleIntoMap(Tuple tuple) {
        Map result = null
        Set entries
        if (tuple) {
            result = new HashMap()
            tuple.fields.each { key, value ->
                result[key] = value
            }
       }
        return result

    }

    private Tuple searchWorkingStores(Template template, Transaction txn=null) {
        // search all working stores except the one for txn for a match
        Tuple match
        TupleStore workingStore
        for (entry in workingStores) {
            if (txn == null || !entry.key == txn) {
                workingStore = entry.value
                match = workingStore.getMatch(template, false)
                if (match) break
            }
        }

        return match
    }

    private long doWait(Template template, long timeout, long timeToWait, long start) {
        if (timeout == NO_WAIT)
            return 0
        else if (timeout == WAIT_FOREVER) {
            template.wait(20) 	// wait as long as required...
            return WAIT_FOREVER
        }
        else if (timeToWait <= 0)
            // time is up
            return 0
        else {
            template.wait(timeToWait)
            // subtract time we've already waited from the total timeout
            timeToWait = timeout - (System.currentTimeMillis() - start)
            return timeToWait
        }

    }
    // TODO: improve on this output; what we really want is the queue length for each key, not each actual tuple/template
    protected String getStats() {
        return "Space: $name\n Tuples: ${tupleMap.inspect()}\n Templates: ${templateMap.inspect()}"
    }

}

