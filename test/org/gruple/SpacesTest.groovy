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
 * SpacesTest.groovy
 * Created on Apr 7, 2009
 */

package org.gruple

/**
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class SpacesTest extends GroovyTestCase {

    static TUPLE1 = [fname:"Vanessa", lname:"Williams", email:"vanessa@fridgebuzz.com", age:22]
    static TEMPLATE1 = [fname:"Vanessa", lname:null, email:"vanessa@fridgebuzz.com", age:22]

    static Tuple template1, tuple1

    void setUp() {
        template1 = new Tuple(TEMPLATE1)
        tuple1 = new Tuple(TUPLE1)
    }


    void testService() {

        Space defaultSpace = Spaces.getSpace()
        assertNotNull(defaultSpace)
        assertEquals(defaultSpace.name, "default")

        Space fooSpace = Spaces.getSpace("foo")
        assertNotNull(fooSpace)
        Space barSpace = Spaces["bar"]
        assertNotNull(barSpace)
        Space bazSpace = Spaces.baz
        assertNotNull(bazSpace)

        defaultSpace.put(tuple1)
        fooSpace.put(tuple1)
        println("getting tuple1 from default")
        assertEquals(tuple1, defaultSpace.get(template1))
        
        Spaces.close()
        assertNull(defaultSpace.get(template1))
        Spaces.closeAll()
        assertNull(fooSpace.get(template1))
        
    }
	
}

