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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.logging.*
import groovy.time.TimeDuration

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
    // TODO: implement transactions
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
    public static final Long WAIT_FOREVER = Long.MAX_VALUE

    /**
     * Convenience constant to indicate indefinite ttl for put tuples.
     * This is the default value.
     */
    public static final Long FOREVER = Long.MAX_VALUE

    /**
    * The space name. Required to look up a reference to a particular space.
    */
    final String name

    /*
    * A Map for holding Lists of Tuples.
    */
    private Map tupleMap

    /*
    * A Map for holding Lists of Templates waiting to be matched.
    */
    private Map templateMap

    private volatile boolean shuttingDown = false

    private Timer expiryTimer

    // Lock object to make certain no two threads get the same match
    private Object matchLock = new Object()

    /**
     * Creates a new instance of TupleSpace.
     *
     * Not called by applications, use SpaceService.getSpace(name) instead.
     *
     */
    protected Space(String name) {
        this.name = name
        tupleMap = new ConcurrentHashMap()
        templateMap = new ConcurrentHashMap()

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
     * @param the tuple's time-to-live in milliseconds. Long.MAX_VALUE(=FOREVER)
     *        is the default
     */
    void put(Map tupleFields, Long ttl=FOREVER) {
        put(new Tuple(tupleFields), ttl)
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
     */
    void put(Map tupleFields, TimeDuration ttl) {
        put(tupleFields, ttl.toMilliseconds())
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
     *          Long.MAX_VALUE(=WAIT_FOREVER)
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map take(Map templateFields, Long timeout=WAIT_FOREVER) throws InterruptedException {

        if (templateFields == null)
            throw new IllegalArgumentException("Field map may not be null.")

        Tuple match = take(new Tuple(templateFields), timeout)
        Map result = null
        if (match)
            result = match.fields
        return result
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
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map take(Map templateFields, TimeDuration timeout) throws InterruptedException {
        return take(templateFields, timeout.toMilliseconds())
    }

    /**
     * Non-destructive read of a tuple which matches the template. Templates
     * use null as wildcard field values.
     *
     * @param a Map of template fields (must adhere to rules for {@Link Tuple})
     * @param timeout in milliseconds; 0 returns immediately; default is
     *        Long.MAX_VALUE(=WAIT_FOREVER)
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map get(Map templateFields, Long timeout=WAIT_FOREVER) throws InterruptedException {

        if (templateFields == null)
            throw new IllegalArgumentException("Field map may not be null.")
            
        Map result = null
        Tuple match = get(new Tuple(templateFields), timeout)
        if (match)
            result = match.fields
        return result
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
     * @throws InterruptedException (for the sake of Java clients)
     * @return a Map of the matched Tuple's fields or null if no match found
     */
    Map get(Map templateFields, TimeDuration timeout) throws InterruptedException {
        return get(templateFields, timeout.toMilliseconds())
    }

    /**
     * Insert a tuple into the space. Applications should use the put(Map, Long) and put(Map, Duration versions).
     *
     * @param the tuple to insert. May not be null.
     * @param the tuple's time-to-live in milliseconds. Long.MAX_VALUE(=FOREVER)
     *        is the default
     */
    protected void put(Tuple tuple, Long ttl=FOREVER) {

        if (!tuple)
            throw new IllegalArgumentException("Attempt to put a null tuple")

        if (tuple.hasFormals())
            throw new IllegalArgumentException("Can't put a template/anti-tuple into the space")
        
        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Putting tuple $tuple")


        store(tuple, tupleMap)
        if (ttl != FOREVER) expiryTimer.runAfter(ttl as int) { remove(tuple) }

        // search the template map. If a match is found, notify() the
        // waiting thread(s).
        def thash = tuple.tupleHash()
        def templates
        if (templateMap.containsKey(thash)) {
            templates = templateMap[thash]
            Tuple antiTuple
            for (Template template in templates) {
                antiTuple = template.tuple
                // XXX: alternative strategy: set all templates' fields, collect in list, shuffle, notify
                if (tuple.matches(antiTuple)) {
                    synchronized(template) {
                        template.notify()
                    }
                    if (template.destructive)
                        break // only notify up to and including the first "take" template
                }
            }
        }

    }

    /**
     * Consume a tuple which matches the template. Templates use null as
     * wildcard field values.
     *
     * Applications should use the take(Map, Long) and take(Map, Duration) versions.
     *
     * @param a template to match.
     * @param timeout time to block for; 0 returns immediately,
     *        Long.MAX_VALUE(=WAIT_FOREVER) is the default. Other values are
     *        in milliseconds.
     * @throws InterruptedException (for the sake of Java clients)
     * @return found tuple, or null if none found within timeout
     */
    protected Tuple take(Tuple antiTuple, Long timeout=WAIT_FOREVER) throws InterruptedException {

        if (!antiTuple)
            throw new IllegalArgumentException("Template may not be null.")

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Taking with template $antiTuple and timeout=$timeout")

        Template template = new Template(antiTuple, true)
        store(template, templateMap)

        Tuple match
        long start = System.currentTimeMillis()
        long timeToWait = timeout
        synchronized (template) {
            //while (!shuttingDown && !(match = getMatch(antiTuple, true))) {
            while (!shuttingDown && !(match = getMatch(template, true))) {
                if (timeToWait <= 0)
                    break
                if (timeout == WAIT_FOREVER)
                    template.wait() 	// wait as long as required...
                else {
                    template.wait(timeToWait)
                    // subtract time we've already waited from the total timeout
                    timeToWait = timeout - (System.currentTimeMillis() - start)
                }
            }
        }
        /*
        if (match) {
            removeTemplate(template)
        }
        */
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
     *        Long.MAX_VALUE(=WAIT_FOREVER) is the default. Other values are
     *        in milliseconds
     * @throws InterruptedException (for the sake of Java clients)
     * @return the matched Tuple or null if none found
     */
    protected Tuple get(Tuple antiTuple, Long timeout=WAIT_FOREVER) throws InterruptedException {

        if (!antiTuple)
            throw new IllegalArgumentException("Template may not be null.")

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Getting with template $antiTuple and timeout=$timeout")

        // store a wrapped template tuple
        Template template = new Template(antiTuple, false)
        store(template, templateMap)

        Tuple match
        long start = System.currentTimeMillis()
        long timeToWait = timeout
        synchronized (template) {
            //while (!shuttingDown && !(match = getMatch(antiTuple, false))) {
            while (!shuttingDown && !(match = getMatch(template, false))) {
                if (timeToWait <= 0)
                    break
                if (timeout == WAIT_FOREVER)
                    template.wait() 	// wait as long as required...
                else {
                    template.wait(timeToWait)
                    // subtract time we've already waited from the total timeout
                    timeToWait = timeout - (System.currentTimeMillis() - start)
                }
            }
        }
        /*
        if (match) {
            removeTemplate(template)
        }
        */
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

            def queues = templateMap.values()
            for (queue in queues) {
                for (Tuple template in queue) {
                    synchronized(template) {
                        template.notify() // wake everyone up
                    }
                }

                templateMap.remove(queue)
                queue.clear()
            }
            templateMap.clear()

            queues = tupleMap.values()
            for (queue in queues) {
                tupleMap.remove(queue)
                queue.clear()
            }
            tupleMap.clear()
        }

    }

    private store(tuple, Map store) {

        assert tuple
        assert store != null

        /*
        Use the tuple type hash to access the List
        to contain this tuple. If the appropriate List
        doesn't exist, create it.
        */

        def thash = tuple.tupleHash()
        def tupleQueue
        if (store.containsKey(thash)) {
            tupleQueue = store[thash]
        }
        else {
            tupleQueue = new ConcurrentLinkedQueue()
            store[thash] = tupleQueue
        }
        tupleQueue << tuple

        if (LOG.isLoggable(Level.FINE)) {
            String storeType = (store == tupleMap) ? "tuple" : "template"
            LOG.fine("Stored  $storeType $tuple")
        }
    }

    private remove(Tuple tuple) {

        assert tuple

        def thash = tuple.tupleHash()
        if (tupleMap.containsKey(thash)) {
            def tuples = tupleMap[thash]
            if (tuples.contains(tuple))
                tuples.remove(tuple)
        }
    }

    //private Tuple getMatch(Tuple template, Boolean destroy) {
    private Tuple getMatch(Template template, Boolean destroy) {

        assert template
        assert destroy != null
        
        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Finding match for template $template Destroy is $destroy")

        def tuple
        def thash = template.tupleHash()
        Collection tuples
        if (tupleMap.containsKey(thash)) {

            synchronized(matchLock) { // Don't allow two threads to match the same tuple

                tuples = tupleMap[thash]

                Collection allMatches
                // XXX: deliberately being non-deterministic may be too expensive
                allMatches = tuples.findAll {
                    it.matches(template)
                }
                if (allMatches.size() > 1)
                    tuple = allMatches.toArray()[random(allMatches.size()-1)] as Tuple
                else if (allMatches)
                    tuple = allMatches.toArray()[0] as Tuple

                /* Deterministic option
                tuple = tuples.find {
                    it.matches(template)
                }
                */

                if (tuple) {

                    if (LOG.isLoggable(Level.FINE))
                        LOG.fine("Matching tuple found $tuple")

                    if (destroy) {
                        assert tuples != null && !tuples.isEmpty() && tuples.contains(tuple)
                        tuples.remove(tuple) // extract the tuple if appropriate
                        if (tuples.isEmpty())
                            tupleMap.remove(thash) // clean up map
                    }

                    // dispose of the template
                    removeTemplate(template)
                }
            }
        }


        return tuple as Tuple
    }

    private removeTemplate(Template template) {

        def thash = template.tuple.tupleHash()
        if (templateMap.containsKey(thash)) {
            def templates = templateMap[thash]
            assert templates.contains(template)
            templates.remove(template)
            if (templates.isEmpty())
                templateMap.remove(thash) // clean up map
        }
    }

    private int random(int max) {
        if (max == 0) return 0
        int result = Math.round(Math.random()*max)
        // just in case rounding causes boundary violation
        if (result < 0) return 0
        if (result > max) return max
        return result
    }

    // TODO: improve on this output; what we really want is the queue length for each key, not each actual tuple/template
    protected String getStats() {
        return "Space: $name\n Tuples: ${tupleMap.inspect()}\n Templates: ${templateMap.inspect()}"
    }
	
}

