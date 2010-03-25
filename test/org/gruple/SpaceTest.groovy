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
 * SpaceTest.groovy
 * Created on Apr 19, 2009
 */

package org.gruple

import org.codehaus.groovy.runtime.TimeCategory
import java.util.logging.Logger
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch

/**
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class SpaceTest extends GroovyTestCase {

    private static final Logger LOG = Logger.getLogger(SpaceTest.name)

    private static final fieldTypes =
        [Integer.class, Short.class, Long.class, Byte.class,
         Float.class, Double.class, BigInteger.class, BigDecimal.class,
         Boolean.class, Character.class, String.class, Date.class]

    // characters used by randomChar
    private static final alphaNumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    private static final chars = alphaNumeric.toCharArray()
    private static final numChars = alphaNumeric.size()

    private tuples = [], templates = []
    private Space space
    


    void setUp() {

        Map fields
        (0..100).each {
            fields = genTupleFields()
            tuples << fields
            templates << makeSomeFormals(fields)
        }
        space = Spaces.getSpace("test")
    }

    void testSpace() {

        Long begin = new Date().time
        println "Starting space test at $begin"

        try {
            space.put(null)
            fail "Expected IllegalArgumentException"
        } catch (IllegalArgumentException success) {}

        try {
            space.put(templates[0])
            if (new Tuple(templates[0]).hasFormals()) fail "Expected IllegalArgumentException"
        } catch (IllegalArgumentException success) {}
        
        0.upto(100) {
            space.put(tuples[it])
        }

        Map tuple
        100.downto(50) {
            tuple = space.get(templates[it])
            assertNotNull(tuple)
        }

        100.downto(50) {
            tuple = space.take(templates[it])
            assertNotNull(tuple)
        }

        49.downto(0) {
            tuple = space.take(templates[it], 10)
            assertNotNull(tuple)
        }

        Thread.sleep(40L)

        49.downto(0) {
            tuple = space.get(templates[it], 10)
            if (tuple != null) {
                System.out.println("Getting tuple $it")
            }
            assertNull(tuple)

        }

        // test expiring tuples
        space.put(tuples[1], 10L)   // tuple has ttl of 10ms
        sleep(50L)                  // wait 20ms to be sure
        tuple = space.take(templates[1] as Map, Space.NO_WAIT)
        if (tuple != null) {
            System.out.println("Timed out tuple is $tuple")
        }
        assertNull(tuple)

        // TODO: test that the space data structures are all empty
        
        Long end = new Date().time
        println "Ending space test at $end"
    }

    void testDurations() {

        use(TimeCategory) {
            space.put(tuples[1], 10.milliseconds)   // tuple has ttl of 10ms
            sleep(20L)                  // wait 20ms to be sure
            Map tuple = space.take(templates[1] as Map, 10.milliseconds)
            assertNull(tuple)
        }

    }

    void testClosures() {
        space.put([price:10])
        def tuple = space.take([price:{it > 5}])
        assertNotNull(tuple)
    }

    void testLeftShift() {
        space << (tuples[2])
        assertNotNull(space.take(templates[2]))
    }


    void testTransactions() {

        // let's start with some really basic stuff involving a single transaction

        // test 1:
        // in txn: in txn take tuple A, do something, put tuple B
        // no txn: read tuple A, should succeed, read tuple B, should fail
        // in txn: commit txn
        // no txn: read tuple A, should fail

        space.put(tuples[2]);

        Transaction txn = new Transaction()

        def tuple = space.take(templates[2], Space.NO_WAIT, txn)
        assertNotNull(tuple)
        tuple = space.get(templates[2], Space.NO_WAIT)
        assertNotNull(tuple)

        space.put(tuples[3], Space.FOREVER, txn)
        tuple = space.get(templates[3], Space.NO_WAIT)
        assertNull(tuple)

        //space.commit(txn)
        txn.commit()

        tuple = space.get(templates[2], Space.NO_WAIT)
        assertNull(tuple)

        // test 2:
        // in txn: take tuple A, do something, put tupble B
        // no txn: take tuple A, should fail, read tuple B should fail
        // in txn: rollback transaction
        // no txn: take tuple A should succeed, read tuple B should fail

        tuple = space.take(templates[3], Space.NO_WAIT, txn)
        assertNotNull(tuple)

        space.put(tuples[4], Space.FOREVER, txn)

        tuple = space.take(templates[3], Space.NO_WAIT)
        assertNull(tuple)

        tuple = space.get(templates[4], Space.NO_WAIT)
        assertNull(tuple)

        //space.rollback(txn)
        txn.rollback()

        tuple = space.take(templates[3], Space.NO_WAIT)
        assertNotNull(tuple)

        tuple = space.get(templates[4], Space.NO_WAIT)
        assertNull(tuple)

    }

    public void testSimpleThreads() {

        def executor = Executors.newFixedThreadPool(3)
        def cdl = new CountDownLatch(3)

        executor.execute {
            doPut(5, 50)
            cdl.countDown()
        }
        executor.execute {
            doTake(5, 25)
            cdl.countDown()
        }
        executor.execute {
            doTake(26, 50)
            cdl.countDown()
        }
        cdl.await ()
    }

    private void doPut(int putFrom, int putTo) {
        List putList = new ArrayList()
        putFrom.upto(putTo) {
            putList.add(it)
        }
        boolean done = false
        int maxWait = 20
        while (!done) {
                boolean wait = Math.random() < 0.75
                if (wait) {
//                    int waitTime = random(maxWait)
//                    sleep(waitTime)
                    Thread.currentThread().yield()
                }
            if (!putList.isEmpty()) {
                int tupleIndex = random(putList.size-1)
                if (tupleIndex < 0) tupleIndex = 0
                int tupleNumber = putList[tupleIndex]
                System.out.println("Thread ${Thread.currentThread().getName()} putting tuple $tupleNumber")
                space.put(tuples[tupleNumber])
                putList.remove(tupleIndex)
            }
            if (putList.isEmpty()) done = true
         }

    }

    private void doTake(int takeFrom, int takeTo) {
        List takeList = new ArrayList()
        takeFrom.upto(takeTo) {
            takeList.add(it)
        }

        boolean done = false
        int maxWait = 20
        while (!done) {
                boolean wait = Math.random() < 0.25
                if (wait) {
                    int waitTime = random(maxWait)
                    sleep(waitTime)
                }
            if (!takeList.isEmpty()) {
                int tupleIndex = random(takeList.size-1)
                if (tupleIndex < 0) tupleIndex = 0
                int tupleNumber = takeList[tupleIndex]
                System.out.println("Thread ${Thread.currentThread().getName()} taking tuple $tupleNumber")
                space.take(templates[tupleNumber])
                takeList.remove(tupleIndex)
            }
            if (takeList.isEmpty()) done = true
        }

    }

    private void doPutAndTake(int putFrom, int putTo, int takeFrom, int takeTo) {

        List putList = new ArrayList()
        putFrom.upto(putTo) {
            putList.add(it)
        }

        List takeList = new ArrayList()
        takeFrom.upto(takeTo) {
            takeList.add(it)
        }

        boolean done = false
        boolean put = false
        while (!done) {
//                boolean wait = Math.random() < 0.25
//                if (wait) {
//                    int waitTime = random(maxWait)
//                    sleep(waitTime)
//                }
            put = Math.random() < 0.5
            if (!put) {
                if (!takeList.isEmpty()) {
                    int tupleIndex = random(takeList.size-1)
                    if (tupleIndex < 0) tupleIndex = 0
                    int tupleNumber = takeList[tupleIndex]
                    System.out.println("Thread ${Thread.currentThread().getName()} taking tuple $tupleNumber")
                    space.take(templates[tupleNumber])
                    takeList.remove(tupleIndex)
                }

            }
            else if (!putList.isEmpty()) {
                int tupleIndex = random(putList.size-1)
                if (tupleIndex < 0) tupleIndex = 0
                int tupleNumber = putList[tupleIndex]
                System.out.println("Thread ${Thread.currentThread().getName()} putting tuple $tupleNumber")
                space.put(tuples[tupleNumber])
                putList.remove(tupleIndex)
            }
            if (putList.isEmpty() && takeList.isEmpty()) done = true
         }

    }

    /*
     * Generate a random number of actual fields. Field types are
     * randomly chosen from a valid set, and field values are randomly
     * generated appropriately for the type.
     *
     * @return  a random tuple
     */
    private Map genTupleFields() {

        // generate a number of fields
        int numFields = random(20)
        if (numFields <1) numFields = 1

        Map fields = [:]
        def fieldValue
        (0..<numFields).each {
            fieldValue = randomFieldValue(randomFieldType())
            fields[randomString(10)] = fieldValue
        }
        return fields
    }

    private Map makeSomeFormals(Map fields) {

        // randomly make some members of the map formal, i.e. make their values null
        Map newFields = fields.clone() as Map
        Set keys = newFields.keySet()
        keys.each {
            if (random(1) > 0) newFields[it] = null
        }
        return newFields
    }
    
    private Class randomFieldType() {

         return fieldTypes[random(fieldTypes.size()-1)] as Class
    }

    private def randomFieldValue(Class fieldType) {

        if (!fieldType)
            throw new IllegalArgumentException("Argument fieldType cannot be null")

        def result
        switch (fieldType) {
            case String     :   result = randomString(32); break
            case Character  :   result = randomChar(); break
            case Integer    :   result = (Integer)random(100); break
            case Short      :   result = (Short)random(100); break
            case Long       :   result = (Long)random(100); break
            case Byte       :   result = (Byte)random(100); break
            case BigInteger :   result = (BigInteger)random(100); break
            case Float      :   result = (Float)random(100); break
            case Double     :   result = (Double)random(100); break
            case BigDecimal :   result = (BigDecimal)random(100); break
            case Boolean    :   result = Math.random() >= 0.5; break
            case Date       :   result = new Date(); break
            default         :   throw new IllegalArgumentException("Illegal field type")
                                break

        }
        result
    }

    /**
     * Generate a random alphanumeric string of at most maxChars.
     *
     * @param maxChars - maximum number of characters in the returned string
     * @return random string
     */
    private String randomString(int maxChars) {

        assert maxChars > 0 : "maxChars must be greater than 0"

        def numChars = random(maxChars)
        if (numChars < 1) numChars = 1

        StringBuilder result = new StringBuilder()
        (0..<numChars).each {
            result<<randomChar()
        }
        result.toString()
    }

    /**
     * Return a random character from the set [A-Za-z0-9].
     *
     * @return an alphanumeric character
     */
    private Character randomChar() {
        chars[random(numChars-1)]
    }

    private int random(int max) {
        int result = Math.round(Math.random()*max)
        // just in case rounding causes boundary violation
        if (result < 0) return 0
        if (result > max) return max
        return result
    }
}

