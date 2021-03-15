/*
 * The MIT License
 *
 * Copyright 2021 randalkamradt.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package net.kamradtfamily.usedvehicles.commonobjects;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 * @author randalkamradt
 */
public final class DefaultEnvironmentProperties 
        implements EnvironmentProperties {
    private final Map<String, String> map;
    private final static Pattern REPLACEMENT_PATTERN 
            = Pattern.compile("([$][$])|([$][{].*[}])|([$]\\w+)");
    
    public DefaultEnvironmentProperties() throws IOException {
        Map<String, String> tempMap;
        try(InputStream is = DefaultEnvironmentProperties
                .class
                .getResourceAsStream("/application.properties")) {
            Properties properties = new Properties();
            properties.load(is);
            tempMap = properties
                    .entrySet()
                    .stream()
                    .collect(Collectors
                            .toUnmodifiableMap(
                                    e -> (String)e.getKey(), 
                                    e -> (String)e.getValue()));

        } catch(IOException ex3) {
            tempMap = new HashMap<>();
        }
        map = resolvePlaceHolders(addEnvironment(tempMap));
    }

    @Override
    public Optional<String> getEnvironmentProperties(String key) {
        return Optional.ofNullable(map.get(key));
    }

    private static Map<String, String> resolvePlaceHolders(
            final Map<String, String> mapin) {
        return mapin.entrySet()
            .stream()
            .collect(Collectors
                    .toUnmodifiableMap(
                            Map.Entry::getKey, 
                            e -> replaceValue(e.getValue(), mapin)));
    }
    
    private static String replaceValue(
            final String value, 
            final Map<String, String> mapin) {
        return REPLACEMENT_PATTERN.matcher(value).replaceAll(mr -> {
            String ret = null;
            String item = mr.group();
            if("$$".equals(item)) {
                ret = "\\$";
            } else if(item.startsWith("${") && item.endsWith("}")) {
                ret = mapin.get(item.substring(2, item.length()-1));
            } else if(item.length() > 1) {
                ret = mapin.get(item.substring(1));
            }
            if(ret == null) {
                throw new RuntimeException(
                        "Unable to find replacement value for mr "
                                + mr.group());
            }
            ret = replaceValue(ret, mapin);
            return ret;
        });
     }
    
    private static Map<String, String> addEnvironment(
            final Map<String, String> mapin) {
        Map<String, String> temp = new HashMap<>(mapin);
        System.getenv().entrySet().forEach(e -> {
            String mappedKey = e.getKey().toLowerCase().replace('_', '.');
            temp.put(mappedKey, e.getValue());
        });
        return Map.ofEntries(temp.entrySet()
                .toArray(new Map.Entry[temp.entrySet().size()]));
    }
    
}
