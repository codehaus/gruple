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
 * Transaction.groovy
 * Created on Nov 24, 2009
 */

package org.gruple

/**
 * Class to implement a Transaction.
 */
public class Transaction {
    // TODO: rollback transactions which have been here too long
    List spaces = Collections.synchronizedList(new ArrayList())

    String id

    public Transaction(String id) {
        this.id = id
    }
    
    /**
     * Enroll a space in this transaction
     * @param the space to enroll
     */
    public enrollSpace(Space space) {
        if (!spaces.contains(space))
            spaces.add(space)
    }

    /**
     * Commit the transaction
     */
    public commit() {
        for (space in spaces) space.commit(this)
        spaces.clear()
    }

    /**
     * Rollback the transaction
     */
    public rollback() {
        for (space in spaces) space.rollback(this)
        spaces.clear()
    }


}