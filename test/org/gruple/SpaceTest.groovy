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

/**
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class SpaceTest extends GroovyTestCase {

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
        space = Spaces.getSpace()
    }

    void testSpace() {

        Long begin = new Date().time
        println "Starting space test at $begin"

        try {
            space.put(null)
            fail "Expected IllegalArgumentException"
        } catch (Exception success) {}

        try {
            space.put(templates[0])
            if (templates[0].hasFormals()) fail "Expected IllegalArgumentException"
        } catch (Exception success) {}
        
        0.upto(100) {
            space.put(tuples[it])
        }

        Map tuple
        100.downto(50) {
            tuple = space.get(templates[it] as Map)
            assertNotNull(tuple)
        }

        100.downto(50) {
            tuple = space.take(templates[it] as Map)
            assertNotNull(tuple)
        }

        49.downto(0) {
            tuple = space.take(templates[it] as Map, 10)
            assertNotNull(tuple)
        }

        49.downto(0) {
            tuple = space.get(templates[it] as Map, 10)
            assertNull(tuple)
        }

        // test expiring tuples
        space.put(tuples[1], 10L)   // tuple has ttl of 10ms
        sleep(20L)                  // wait 20ms to be sure
        tuple = space.take(templates[1] as Map,0)
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

    void testLeftShift() {
        space << (tuples[2])
        assertNotNull(space.take(templates[2]))
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

