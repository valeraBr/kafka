/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Objects;
import java.util.LinkedList;

public class ConnectSchema implements Schema {
    /**
     * Maps Schema.Types to a list of Java classes that can be used to represent them.
     */
    private static final Map<Type, List<Class>> SCHEMA_TYPE_CLASSES = new HashMap<>();
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the Java classes to the corresponding Schema.Type.
     */
    private static final Map<Class<?>, Type> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        SCHEMA_TYPE_CLASSES.put(Type.INT8, Arrays.asList((Class) Byte.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT16, Arrays.asList((Class) Short.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Arrays.asList((Class) Integer.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Arrays.asList((Class) Long.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Arrays.asList((Class) Float.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Arrays.asList((Class) Double.class));
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Arrays.asList((Class) Boolean.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRING, Arrays.asList((Class) String.class));
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        SCHEMA_TYPE_CLASSES.put(Type.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class));
        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, Arrays.asList((Class) List.class));
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Arrays.asList((Class) Map.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Arrays.asList((Class) Struct.class));

        for (Map.Entry<Type, List<Class>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
            for (Class<?> schemaClass : schemaClasses.getValue())
                JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
        }

        LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Arrays.asList((Class) BigDecimal.class));
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Arrays.asList((Class) java.util.Date.class));
        // We don't need to put these into JAVA_CLASS_SCHEMA_TYPES since that's only used to determine schemas for
        // schemaless data and logical types will have ambiguous schemas (e.g. many of them use the same Java class) so
        // they should not be used without schemas.
    }

    // The type of the field
    private final Type type;
    protected final boolean optional;
    protected final Object defaultValue;

    private final List<Field> fields;
    private final Map<String, Field> fieldsByName;

    private final Schema keySchema;
    private final Schema valueSchema;

    // Optional name and version provide a built-in way to indicate what type of data is included. Most
    // useful for structs to indicate the semantics of the struct and map it to some existing underlying
    // serializer-specific schema. However, can also be useful in specifying other logical types (e.g. a set is an array
    // with additional constraints).
    protected final String name;
    private final Integer version;
    // Optional human readable documentation describing this schema.
    private final String doc;
    private final Map<String, String> parameters;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc, Map<String, String> parameters, List<Field> fields, Schema keySchema, Schema valueSchema) {
        this.type = type;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        this.parameters = parameters;

        this.fields = fields;
        if (this.fields != null && this.type == Type.STRUCT) {
            this.fieldsByName = new HashMap<>();
            for (Field field : fields)
                fieldsByName.put(field.name(), field);
        } else {
            this.fieldsByName = null;
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;

        resolve(null);
    }

    /**
     * Update the parent list and resolve all the child schema's
     * @param parents a list of schemas that are parents of this schema
     * @return the resolved schema
     */
    @Override
    public Schema resolve(List<Schema> parents) {
        if (parents == null) parents = new ArrayList();
        parents.add(this);

        if (fields != null) {
            for (Field field : fields) {
                field.schema().resolve(new ArrayList<>(parents));
            }
        }

        if (keySchema != null) keySchema.resolve(new ArrayList<>(parents));
        if (valueSchema != null) valueSchema.resolve(new ArrayList<>(parents));

        return this;
    }

    /**
     * Construct a Schema for a primitive type, setting schema parameters, struct fields, and key and value schemas to null.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc) {
        this(type, optional, defaultValue, name, version, doc, null, null, null, null);
    }

    /**
     * Construct a default schema for a primitive type. The schema is required, has no default value, name, version,
     * or documentation.
     */
    public ConnectSchema(Type type) {
        this(type, false, null, null, null, null);
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public String doc() {
        return doc;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fieldsByName.get(fieldName);
    }

    @Override
    public Schema keySchema() {
        if (type != Type.MAP)
            throw new DataException("Cannot look up key schema on non-map type");
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        if (type != Type.MAP && type != Type.ARRAY)
            throw new DataException("Cannot look up value schema on non-array and non-map type");
        return valueSchema;
    }

    /**
     * Validate that the value can be used with the schema, i.e. that its type matches the schema type and nullability
     * requirements. Throws a DataException if the value is invalid.
     * @param schema Schema to test
     * @param value value to test
     */
    public static void validateValue(Schema schema, Object value) {
        if (value == null) {
            if (!schema.isOptional())
                throw new DataException("Invalid value: null used for required field");
            else
                return;
        }

        List<Class> expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.name());

        if (expectedClasses == null)
                expectedClasses = SCHEMA_TYPE_CLASSES.get(schema.type());

        if (expectedClasses == null)
            throw new DataException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());

        boolean foundMatch = false;
        for (Class<?> expectedClass : expectedClasses) {
            if (expectedClass.isInstance(value)) {
                foundMatch = true;
                break;
            }
        }
        if (!foundMatch)
            throw new DataException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    throw new DataException("Struct schemas do not match.");
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                for (Object entry : array)
                    validateValue(schema.valueSchema(), entry);
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.keySchema(), entry.getKey());
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    /**
     * Validate that the value can be used for this schema, i.e. that its type matches the schema type and optional
     * requirements. Throws a DataException if the value is invalid.
     * @param value the value to validate
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }

    @Override
    public Schema schema() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return compareContext(new LinkedList<Object>()).equals(o);
    }

    @Override
    public int hashCode() {
        return compareContext(new LinkedList<Object>()).hashCode();
    }

    @Override
    public String toString() {
        if (name != null)
            return "Schema{" + name + ":" + type() + "}";
        else
            return "Schema{" + type() + "}";
    }

    @Override
    public Object compareContext(LinkedList<Object> context) {
        return new ComparisonContext(context);
    }

    /**
     * Get the {@link Schema.Type} associated with the given class.
     *
     * @param klass the Class to
     * @return the corresponding type, nor null if there is no matching type
     */
    public static Type schemaType(Class<?> klass) {
        synchronized (JAVA_CLASS_SCHEMA_TYPES) {
            Type schemaType = JAVA_CLASS_SCHEMA_TYPES.get(klass);
            if (schemaType != null)
                return schemaType;

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_SCHEMA_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_SCHEMA_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }

    /**
     * Add thread safe context information in order to compare potentially cyclic
     * schemas. The context maintains a list of already compared nodes to detect
     * cycles and handle them appropriately.
     */
    class ComparisonContext {
        private final LinkedList<Object> context;

        public ComparisonContext(LinkedList<Object> context) {
            this.context = context;
        }

        /**
         * Compare the schema to the provided object, using the context to break cyclic comparisons.
         * @param o
         * @return
         */
        @Override
        public boolean equals(Object o) {
            for (Object c : context) {
                if (c == ConnectSchema.this) return true;
            }
            if (this == o) return true;
            if (o == null || !(o instanceof Schema)) return false;
            Schema schema = (Schema) o;

            context.addFirst(ConnectSchema.this);
            boolean result =  Objects.equals(isOptional(), schema.isOptional()) &&
                    Objects.equals(type(), schema.type()) &&
                    Objects.equals(defaultValue(), schema.defaultValue()) &&
                    Objects.equals(name(), schema.name()) &&
                    Objects.equals(version(), schema.version()) &&
                    Objects.equals(doc(), schema.doc()) &&
                    Objects.equals(parameters(), schema.parameters()) &&
                    (type() != Type.STRUCT || Objects.equals(fieldsWithContext(), schema.fields())) &&
                    (type() != Type.MAP || Objects.equals(keySchema().compareContext(context), schema.keySchema())) &&
                    ((type() != Type.MAP && type() != Type.ARRAY) || Objects.equals(valueSchema().compareContext(context), schema.valueSchema()));
            context.removeFirst();

            return result;
        }

        /**
         * Get the hashcode of the schema, using the context to break cyclic hashing.
         * @return
         */
        @Override
        public int hashCode() {
            for (Object c : context) {
                if (c == ConnectSchema.this) return 0;
            }

            context.addFirst(this);
            int result =  Objects.hash(
                isOptional(),
                type(),
                defaultValue(),
                (type() == type.STRUCT) ? fieldsWithContext() : null,
                (type() == type.MAP) ? keySchema().compareContext(context) : null,
                (type() == type.MAP || type() == type.ARRAY) ? valueSchema().compareContext(context) : null,
                version(),
                doc(),
                parameters());
            context.removeFirst();

            return result;
        }

        /**
         * Remap the list of fields to include the context of the comparison. This allows the
         * fields to use the context when comparing their schema's.
         * @return A new list of the fields comparison contexts
         */
        private List<Object> fieldsWithContext() {
            if (fields() == null) return null;
            List<Object> result = new ArrayList();
            for (Field field: fields()) {
                result.add(field.compareContext(context));
            }
            return result;
        }
    }
}
