package org.gruple

import org.gruple.Tuple

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
 * Template.groovy
 * Created on May 16, 2009
 */

/**
 * Wrapper for Tuples stored in Space templateMap. Adds some extra fields and delegates most
 * methods to Tuple
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
class Template {

    /**
     * True for "take" template, false for "get" templates
     */
    final Boolean destructive

    /**
    * Id of the thread that owns this template. Used to uniquely identify template to
    * remove from the templateMap when a match is found.
    */
    final String uniqueId


    /*
    * Delegate most methods to this tuple
    */
    @Delegate final Tuple tuple

    /**
     * Constructor.
     */
    Template (Tuple tuple, Boolean destructive) {
        this.tuple = tuple
        this.destructive = destructive
        // create a unique identifier for this template from the thread id and time
        String now = System.currentTimeMillis()
        String postfix = now[now.size()-6..now.size()-1]
        String random = Math.round(Math.random() * 32767)
        this.uniqueId = Thread.currentThread().id + postfix +random
    }

    @Override
    boolean equals(o) {

        if (this.is(o)) return true
        if (!o || this.class != o.class) return false

        if (destructive != o.destructive) return false
        if (uniqueId != o.uniqueId) return false
        if (tuple != o.tuple) return false
        return true
    }

    @Override
    int hashCode() {
        int result
        result = (destructive ? 1 : 0)
        result = 31 * result + uniqueId.hashCode()
        result = 31 * result + tuple.hashCode()
        return result
    }

}