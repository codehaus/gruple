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
 * MemoryTupleStore.groovy
 * Created on Nov 24, 2009
 */

package org.gruple

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.logging.Level
import java.util.logging.Logger

/**
 * An implementation of a TupleStore which keeps all tuples and
 * templates in memory.
 *
 * @see org.gruple.TupleStore
 */
public class MemoryTupleStore implements TupleStore {

    private static final Logger LOG = Logger.getLogger(MemoryTupleStore.name)

    /*
    * A Map for holding Lists of Tuples.
    */
    private final Map tupleMap = new ConcurrentHashMap()

    /*
    * A Map for holding Lists of Templates waiting to be matched.
    */
    private final Map templateMap = new ConcurrentHashMap()

    public void storeTuple(Tuple tuple) {

        assert tuple

        /*
        Use the tuple type hash to access the List
        to contain this tuple. If the appropriate List
        doesn't exist, create it.
        */

        def thash = tuple.tupleHash()
        List tupleList
        // use an atomic action to create a new queue if necessary
        tupleMap.putIfAbsent(thash, Collections.synchronizedList(new ArrayList()))
        tupleList = tupleMap[thash]
        // add the tuple at a random location in the list
        if (tupleList.isEmpty()) {
            tupleList << tuple
        }
        else {
            int randomIndex = random(tupleList.size()-1)
            tupleList.add(randomIndex, tuple)
        }

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Stored tuple $tuple")
        }
    }

    public void storeTemplate(Template template) {

        assert template

        /*
        Use the tuple type hash to access the List
        to contain this tuple. If the appropriate List
        doesn't exist, create it.
        */

        def thash = template.tupleHash()
        Queue templateQueue
        // use an atomic action to create a new queue if necessary
        templateMap.putIfAbsent(thash, new ConcurrentLinkedQueue())
        templateQueue = templateMap[thash]
        templateQueue << template

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Stored template $template")
        }



    }

    public void removeTuple(Tuple tuple) {

        assert tuple

        def thash = tuple.tupleHash()
        if (tupleMap.containsKey(thash)) {
            def tuples = tupleMap[thash]
            tuples.remove(tuple)
            if (tuples.isEmpty()) tupleMap.remove(thash)
        }
    }

    public void removeTemplate(Template template) {

        def thash = template.tuple.tupleHash()
        if (templateMap.containsKey(thash)) {
            def templates = templateMap[thash]
            templates.remove(template)
            if (templates.isEmpty())
                templateMap.remove(thash) // clean up map
        }
    }

    public Tuple getMatch(Template template, Boolean destroy) {
        
        assert template
        assert destroy != null

        if (LOG.isLoggable(Level.FINE))
            LOG.fine("Finding match for template $template Destroy is $destroy")

        def tuple
        def thash = template.tupleHash()
        List tuples
        if (tupleMap.containsKey(thash)) {

            tuples = tupleMap[thash]

            if (destroy) {

                synchronized(tuples) {
                    tuple = tuples.find {
                        it.matches(template.tuple)
                    }
                    if (tuple) tuples.remove(tuple)
                }

                if (tuple) {
                    if (LOG.isLoggable(Level.FINE))
                        LOG.fine("Matching tuple found $tuple")

                    if (tuples.isEmpty()) tupleMap.remove(thash) // clean up map

                    // dispose of the template
                    removeTemplate(template)
                }

            }
            else {

                synchronized(tuples) {
                    tuple = tuples.find {
                        it.matches(template.tuple)
                    }
                }

                if (tuple) {
                    if (LOG.isLoggable(Level.FINE))
                        LOG.fine("Matching tuple found $tuple")

                    // dispose of the template
                    removeTemplate(template)
                }

            }

        }
        return tuple as Tuple
    }

    public List getWaitingTemplates(Tuple tuple) {

        def thash = tuple.tupleHash()
        def templates
        List results = new ArrayList()
        if (templateMap.containsKey(thash)) {
            templates = templateMap[thash]
            Tuple antiTuple
            for (Template template in templates) {
                antiTuple = template.tuple
                if (tuple.matches(antiTuple)) {
                    results.add(template)
                    if (template.destructive)
                        break // only notify up to and including the first "take" template
                }
            }
        }
        return results
    }

    List getAllTuples() {
        List result = new ArrayList()
        tupleMap.each { key, value ->
            result.add(value)
        }
        return result
    }



    public synchronized void deleteStorage() {

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


    private int random(int max) {
        return Math.round(Math.random()*max) as int
    }

}