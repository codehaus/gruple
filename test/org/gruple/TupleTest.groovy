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
 * TupleTest.groovy
 * Created on Apr 6, 2009
 */

package org.gruple

/**
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class TupleTest extends GroovyTestCase {

    static TEMPLATE1 = [fname:"Vanessa", lname:null, email:"vanessa@fridgebuzz.com", age:22]
    static TEMPLATE2 = [city:"Toronto", province:"Ontario", country:null]
    static TEMPLATE3 = [lname:"Williams", email:null, fname:"Vanessa", age:null]
    static TUPLE1 = [fname:"Vanessa", lname:"Williams", email:"vanessa@fridgebuzz.com", age:22]
    static MUTABLE = [fname:"Vanessa", color: new StringBuilder()]
    static WITHCOLLECTION = [fname:"Vanessa", bday:[1, 2, 3]]
    static WITHMUTABLECOLLECTION = [fname:"Vanessa", foo:[1,2,[3, new URL("http://java.sun.com/index.html")]]]
    static WITHDATE = [date:new Date()]

    static Tuple template1, template2, template3, template4, template5, tuple1, tuple2, tuple3, mutable

    void setUp() {
        template1 = new Tuple(TEMPLATE1)
        template2 = new Tuple(TEMPLATE2)
        template3 = new Tuple(TEMPLATE3)
        template4 = new Tuple(TEMPLATE1)
        template5 = new Tuple(TEMPLATE1)
        tuple1 = new Tuple(TUPLE1)
        tuple2 = new Tuple(TUPLE1)
        tuple3 = new Tuple(TUPLE1)
    }

    void testConstruct() {
        assertNotNull(template1)
        assertNotNull(tuple1)
        try {
            new Tuple(MUTABLE)
            fail("Expected IllegalArgumentException")
        } catch (IllegalArgumentException success) {}

        // arrays are currently not allowed
        Integer[] x = [1, 2]
        try {
            new Tuple([foo: x])
            fail "Expected IllegalArgumentException"
        } catch (Exception success) {}

        // null and empty fields should fail
        try {
            new Tuple(null)
            fail "Expected IllegalArgumentException"
        } catch (Exception success) {}
        try {
            new Tuple([:])
            fail "Expected IllegalArgumentException"
        } catch (Exception success) {}
    }

    void testMatches() {
        assertTrue(tuple1.matches(template1))
        assertFalse(tuple1.matches(template2))
        assertTrue(tuple1.matches(tuple1)) // tuple matches itself
        try {
            template1.matches(tuple1) // can only call on a tuple (no formal fields)
            fail("Expected AssertionError")
        } catch (java.lang.AssertionError success) {}
        assertTrue(tuple1.matches(template3))
    }

    void testEquals() {

        assertEquals(tuple1, tuple1); // reflexivity
        assertEquals(template1, template1);

        assertEquals(tuple1, tuple2); //symmetry
        assertEquals(tuple2, tuple1);
        assertEquals(template1, template4);
        assertEquals(template4, template1);

        assertEquals(tuple2, tuple3); // transitivity
        assertEquals(tuple1, tuple3);
        assertEquals(template4, template5);
        assertEquals(template1, template5);

        assertFalse(template1.equals(tuple1));
        assertFalse(tuple1.equals(null));
    }

    void testHash() {

        assertEquals(tuple1.hashCode(), tuple2.hashCode()); // objects which are equal have same hash
        assertEquals(template1.hashCode(), template4.hashCode());

        assertFalse(tuple1.hashCode() == template1.hashCode()); // unequal objects have unequal hashes
    }

    void testTupleHash() {
        assertEquals(tuple1.tupleHash(), template1.tupleHash())
        assertFalse(tuple1.tupleHash() == template2.tupleHash())
    }

    void testImmutable() {
        try {
            tuple1.fields['fname'] = "Hellen"
            fail "Expected UnsupportedOperationException"
        } catch (Exception success) {}

        Tuple withCollection = new Tuple(WITHCOLLECTION)
        def colFields = withCollection.fields
        try {
            colFields['bday'][0] = 4
            fail "Expected UnsupportedOperationException"
        } catch (Exception success) {}

        // a sub-collection with mutable members is not allowed
        try {
            Tuple withMutableCollection = new Tuple(WITHMUTABLECOLLECTION)
            fail "Expected IllegalArgumentException"
        } catch (Exception success) {}
    }

    void testClosures() {
        // test constuction of templates with Closures
        Tuple closureTemplate = new Tuple([a:{it < 3}])
        // test matching of templates with Closures
        Tuple tuple = new Tuple([a:1])
        Tuple tuple2 = new Tuple([a:4])
        assertTrue(tuple.matches(closureTemplate))
        assertFalse(tuple2.matches(closureTemplate))
        // test tuplehashes for templates with Closures
        Tuple closureTemplate2 = new Tuple([a:{it ==~/J.+/}])
        assertEquals(closureTemplate.tupleHash(), closureTemplate2.tupleHash())

    }

}

