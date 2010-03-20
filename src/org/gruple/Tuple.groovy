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
 * Tuple.groovy
 * Created on Apr 4, 2009
 */

package org.gruple

import java.util.logging.Logger

/**
 * An immutable Tuple data structure. Note that
 * fields in a tuple are named, but not ordered.
 *
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
 * <li>Enum types</li>
 * <li>URI</li>
 * <li>Date (we assume no deprecated methods are ever used)</li>
 * <li>Classes using the @Immutable annotation</li>
 * <li>Collections of any of the above</li>
 * </ul>
 * Note that Arrays are not supported as Tuple fields because they can't be made immutable.
 *
 * @author Vanessa Williams <vanessa@fridgebuzz.com>
 */
final class Tuple implements Serializable {
    
    private static final Logger LOG = Logger.getLogger(Tuple.name)

    private final Map fields

    private Boolean formal = null

    private static final knownImmutables =
        [Integer.class, Short.class, Long.class, Byte.class, Float.class, Double.class, BigInteger.class,
         BigDecimal.class, Boolean.class, Character.class, String.class, URI.class, Date.class]

    /**
     * A Tuple consists of a set of named, but unordered fields. Field
     * names are taken from the Map's keys; field values from the associated
     * values. All field keys should be Strings; all field values must be
     * immutable types. Use the marker interface org.gruple.Immutable if
     * necessary to declare a type as immutable.
     *
     * @param fields - a map of field names and values
     * @throws IllegalArgumentException if any keys are not Strings or values are mutable types
     */
    protected Tuple(Map fields) {

         if (fields == null || fields == [:])
            throw new IllegalArgumentException("Tuple can't be null or have zero fields")

        // check that all keys are Strings and all fields are immutable types
        // Convert all collections to immutable ones
        Map tempFields = [:]
        fields.each { key, value ->
            if (!(key instanceof String)) {
                throw new IllegalArgumentException(
                   "Illegal key ${key.toString()}. All keys must be Strings.".toString())
            }
            if (value != null) {
                if (value.class.isArray()) {
                    throw new IllegalArgumentException(
                        "Illegal entry $key : ${value.toString()}" +
                        "Arrays are not currently supported. Please use a List instead.")
                }
                if (!(value instanceof Closure) && mutableType(value)) {
                    throw new IllegalArgumentException(
                        "Illegal entry $key : ${value.toString()}" +
                        "All Tuple fields must be of immutable type; " +
                        "use lang.groovy.Immutable annotation if necessary")
                }
            }
            // make any Collections immutable
            tempFields[key] = ensureImmutableCollections(value)
        }
        this.fields = new HashMap(tempFields).asImmutable()
    }


    /**
     * @returns a the Tuple's fields (as an immutable Map.)
     */
    protected final Map getFields() {
        return this.fields
    }

    /**
     * Templates and tuples with the same field names and types (classes)
     * must return the same hash value. Field ordering is not important.
     *
     * This is NOT the same as Object.hashCode().
     *
     */
    // XXX: if protected, @Delegate won't find it
    public Integer tupleHash() {

        Integer result = 17;
        fields.keySet().each {
            result += it.hashCode()
            // Tuple field ordering is not important, since in this
            // implementation, fields are named, but not ordered.
        }
        result
    }

    /**
     * Deteremine whether the given anti-tuple is a match for this tuple.
     * A template is a match if:
     * <ul>
     * <li>all fields have the same names and types</li>
     * <li>all non-null fields in the anti-tuple have the same values in the
     * tuple</li>
     * </ul>
     * Note that field order is not significant.
     *
     * @returns true if anti-tuple mathces this tuple, false o/w
     */
    // FIXME: make symmetrical
    protected Boolean matches(final Tuple antiTup){

        if (hasFormals())
            throw new IllegalArgumentException("Cannot call match on a template; no fields may be formal (null): ${this.toString()}")

        Map aFields = antiTup.fields
        assert aFields != [:] : "Antituple can't have zero fields."

        if (fields.size() != aFields.size()) return false // non-matching number of fields
        Set aKeys = aFields.keySet()
        if (fields.keySet() != aKeys) return false // non-matching field names

        // the tuple matches if:
        // - all fields have the same names and types;
        // - all non-null fields in the antituple have the same values
        //   in the tuple
        //   OR any Closure predicates in the antituple evaluate to true when
        //   called with the corresponding tuple field as an argument
        def tValue, aValue
        for (aKey in aKeys) {

            tValue = fields[aKey]
            aValue = aFields[aKey]

            if (aValue != null) {
                if (aValue instanceof Closure) {
                    // FYI, I wrote this line, but I have no longer have any idea how it works
                    if (!aValue.call(tValue)) return false
                } else {
                    if (tValue.class != aValue.class) return false  // non-matching field type
                    if (aValue != tValue) return false  // non-matching non-null field value             
                }
            }
        }
        // if you made it this far, it's a match
        return true

    }

    protected boolean hasFormals() {

        if (this.formal == null) {
            this.formal = (fields.containsValue(null) || fields.any {it instanceof Closure})
        }
        return formal
    }

    /*
    * A not very thorough check for mutability. If the given object
    * is one of the basic immutable types, or if it implements
    * the groovy.lang.Immutable annotation, it's assumed to be
    * immutable. Collections are converted to immutable Collections.
    *
    * @param the object to test for mutability
    * @return true if mutable, false if immutable
    */
    private Boolean mutableType(Object obj) {
        Boolean result = true
        Class fieldType = obj.class
        if (knownImmutables.contains(fieldType) ||
            fieldType.isEnum() ||
            // any of these work
            //(fieldType.annotations.any {it.toString == "@groovy.lang.Immutable()"})) {
            //(fieldType.getAnnotation(Immutable.class) != null)) {
            (fieldType.annotations.any {it.annotationType == Immutable})) {
            result = false
        } else if (obj instanceof Collection) {
            result = false // by default collections are ok
            // recursively look inside collections for mutable types
            for (entry in obj) {
                if (mutableType(entry)) {
                  result = true
                  break // finding one is enough
                }
            }
        } else if (obj instanceof Map) {
            result = false // by default maps are ok
            // recursively look inside maps for mutable types
            for (entry in obj) {
                if (mutableType(entry.key) || mutableType(entry.value)) {
                  result = true
                  break // finding one is enough
                }
            }
        }

        return result
    }

    /*
    * Make all Collections immutable
    */
    private Object ensureImmutableCollections(Object obj) {
        
        def result = obj
        if (obj != null && (obj instanceof Collection || obj instanceof Map)) {
            result = obj.clone().asImmutable()
        } 
        return result
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) return true
        if (!o || this.class != o.class) return false
        if (fields != o.fields) return false
        return true

    }

    @Override
    int hashCode() {
        int result=17
        result = 37*result + fields.hashCode()
        return result
    }

    @Override
    String toString() {

        String tupleType
        if (hasFormals())
            tupleType = "Template"
        else
            tupleType = "Tuple"

        return "$tupleType: ${fields.toMapString()}"
    }

}

