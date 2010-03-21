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
 * TupleStore.groovy
 * Created on Nov 24, 2009
 */

package org.gruple;

/**
 * Interface for underlying storage of Tuples in a Space.
 *
 * @see org.gruple.MemoryTupleStore
 */
public interface TupleStore {

    /**
     * Store the given tuple.
     */
    void storeTuple(Tuple tuple);

    /**
     * Store the given template
     */
    void storeTemplate(Template template);

    /**
     * Remove the given tuple
     */
    void removeTuple(Tuple tuple);

    /**
     * Remove the given template
     */
    void removeTemplate(Template template);

    /**
     * Find a match for the given template
     * @param template tempalte to match
     * @param destroy whether this is a destructive match
     * @return the matched tuple or null
     */
    Tuple getMatch(Template template, Boolean destroy);

    /**
     * Get all templates waiting on a given tuple
     * @param the tuple to test for
     * @return a List of waiting templates
     */
    List getWaitingTemplates(Tuple tuple);

    /**
     * Destroy the storage this class holds
     */
    void deleteStorage();

    /**
     * Get all tuples in the store
     * @return a List of all tuples stored here
     */
    List getAllTuples();

}
