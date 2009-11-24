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
 * Spaces.groovy
 * Created on Apr 4, 2009
 */

package org.gruple

/**
 * This class returns a named Space.
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
final class Spaces {

    /**
    * Holds references to named Spaces
    */
    private static Map spaceMap = new HashMap()

    static Space get(String name) {
        return getSpace(name);
    }

    static getAt(String name) {
        return getSpace(name)
    }

    static get(String name) {
        return getSpace(name)
    }
    
    /**
    * Returns the named TupleSpace. If it doesn't exist, it
    * is created first. If no name is provided, the default
    * space will be returned.
    *
    * @param name The name of the TupleSpace
    * @return a reference to the named TupleSpace
    */
    static synchronized Space getSpace(String name="default") {

        if (spaceMap.containsKey(name)) {
            return spaceMap[name]
        }
        else {
            Space newSpace = new Space(name)
            spaceMap[name] = newSpace
            return newSpace
        }
    }

    /**
     * Close the named space. All resources will be released.
     *
     * @param name the name of the space to close.
     */
    static synchronized void close(String name="default") throws InterruptedException {

        if (spaceMap.containsKey(name)) {
            Space space = spaceMap[name]
            spaceMap.remove(name)
            space.close()
        }
    }

    /**
     * Close all spaces and clean up map.
     */
    static synchronized void closeAll() throws InterruptedException {

        def spaces = spaceMap.values()
        for (Space space in spaces) {
            space.close()
        }
        spaceMap.clear()
    }


}

