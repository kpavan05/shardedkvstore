package dslabs.testsuites;

import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({Lab5Part1TestSuite.class, Lab5Part2TestSuite.class, 
	Lab5Part3TestSuite.class, Lab5Part4TestSuite.class})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public interface Lab5TestSuite {
}
