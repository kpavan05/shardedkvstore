package dslabs.kvstore;

import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.PrettyTestName;
import dslabs.framework.testing.junit.TestPointValue;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import static dslabs.kvstore.KVStoreWorkload.append;
import static dslabs.kvstore.KVStoreWorkload.appendResult;
import static dslabs.kvstore.KVStoreWorkload.get;
import static dslabs.kvstore.KVStoreWorkload.getResult;
import static dslabs.kvstore.KVStoreWorkload.keyNotFound;
import static dslabs.kvstore.KVStoreWorkload.put;
import static dslabs.kvstore.KVStoreWorkload.putOk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(DSLabsTestRunner.class)
public class KVStoreTest {
    private KVStore kvStore;

    @Before
    public void setup() {
        kvStore = new KVStore();
    }

    @Test(timeout = 2 * 1000)
    @PrettyTestName("Test for key-value store deep copy")
    @TestPointValue(5)
    public void test01DeepCopy() throws IllegalAccessException {
        KVStore source = new KVStore();
        source.execute(put("FOO", "BAR"));
        KVStore replica = new KVStore(source);

        // all fields should be private
        Field[] fields = KVStore.class.getDeclaredFields();
        List<Field> privateFieldList = Arrays.stream(fields).filter(field -> Modifier.isPrivate(field.getModifiers())).collect(Collectors.toList());
        assertEquals(fields.length, privateFieldList.size());
        
        for (Field field : privateFieldList) {
            field.setAccessible(true);
            Object sourceField = field.get(source);
            Object replicaField = field.get(replica);
            assertEquals(sourceField, replicaField);
            assertNotSame(sourceField, replicaField);
        }
        
        assertEquals(source, replica);
        assertNotSame(source, replica);
    }

    @Test(timeout = 5 * 1000)
    @TestPointValue(5)
    @PrettyTestName("Basic key-value operations")
    public void test02BasicKVTests() {
        assertEquals(keyNotFound(), kvStore.execute(get("FOO")));
        assertEquals(putOk(), kvStore.execute(put("FOO", "BAR")));
        assertEquals(appendResult("BARBAZ"),
                kvStore.execute(append("FOO", "BAZ")));
        assertEquals(appendResult("BARBAZBAZ"),
                kvStore.execute(append("FOO", "BAZ")));
        assertEquals(appendResult("BAR2"),
                kvStore.execute(append("FOO2", "BAR2")));
        assertEquals(putOk(), kvStore.execute(put("FOO2", "BAZ2")));
        assertEquals(getResult("BAZ2"), kvStore.execute(get("FOO2")));
        assertEquals(putOk(), kvStore.execute(put("fizz", "buzz")));
        assertEquals(getResult("buzz"), kvStore.execute(get("fizz")));
        assertEquals(getResult("BARBAZBAZ"), kvStore.execute(get("FOO")));
        assertEquals(appendResult("BARBAZBAZ[c:1, v:2]"),
                kvStore.execute(append("FOO", "[c:1, v:2]")));
        assertEquals(getResult("BARBAZBAZ[c:1, v:2]"),
                kvStore.execute(get("FOO")));

        String value = RandomStringUtils.randomAscii(1000);
        assertEquals(putOk(), kvStore.execute(put("key", value)));
        assertEquals(getResult(value), kvStore.execute(get("key")));
    }
}

