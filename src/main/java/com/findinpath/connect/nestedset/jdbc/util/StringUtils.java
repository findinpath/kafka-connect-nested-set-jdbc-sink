/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.findinpath.connect.nestedset.jdbc.util;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {

    /**
     * Generate a String by appending all the @{elements}, converted to Strings, delimited by
     *
     * @param <T>      the type of the elements being joined
     * @param elements list of elements to concatenate
     * @param delim    delimiter to place between each element
     * @return the concatenated string with delimiters
     */
    public static <T> String join(Iterable<T> elements, String delim) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (T elem : elements) {
            if (first) {
                first = false;
            } else {
                result.append(delim);
            }
            result.append(elem);
        }
        return result.toString();
    }

    public static boolean isBlank(CharSequence cs) {
        if (cs == null) return true;
        final int strLen = cs.length();
        if (strLen == 0) return true;

        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }

        return true;
    }
}
