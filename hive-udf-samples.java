class SimpleUDFExample extends UDF {
  
  public Text evaluate(Text input) {
    if(input == null) return null;
    return new Text("Hello " + input.toString());
  }
}

// testing class
public class SimpleUDFExampleTest {
  
  @Test
  public void testUDF() {
    SimpleUDFExample example = new SimpleUDFExample();
    Assert.assertEquals("Hello world", example.evaluate(new Text("world")).toString());
  }
  @Test
  public void testUDFNullCheck() {
    SimpleUDFExample example = new SimpleUDFExample();
    Assert.assertNull(example.evaluate(null));
  }  
}

// HIVE testing commands
%> mvn assembly:single
%> hive
hive> ADD JAR target/hive-extensions-1.0-SNAPSHOT-jar-with-dependencies.jar;
hive> CREATE TEMPORARY FUNCTION helloworld as 'com.matthewrathbone.example.SimpleUDFExample';
hive> select helloworld(name) from people limit 1000;

class ComplexUDFExample extends GenericUDF {

  ListObjectInspector listOI;
  StringObjectInspector elementOI;

  @Override
  public String getDisplayString(String[] arg0) {
    return "arrayContainsExample()"; // this should probably be better
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
    }
    // 1. Check we received the right object types.
    ObjectInspector a = arguments[0];
    ObjectInspector b = arguments[1];
    if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
    }
    this.listOI = (ListObjectInspector) a;
    this.elementOI = (StringObjectInspector) b;
    
    // 2. Check that the list contains strings
    if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list of strings");
    }
    
    // the return type of our function is a boolean, so we provide the correct object inspector
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    // get the list and string from the deferred objects using the object inspectors
    List<String> list = (List<String>) this.listOI.getList(arguments[0].get());
    String arg = elementOI.getPrimitiveJavaObject(arguments[1].get());
    
    // check for nulls
    if (list == null || arg == null) {
      return null;
    }
    
    // see if our list contains the value we need
    for(String s: list) {
      if (arg.equals(s)) return new Boolean(true);
    }
    return new Boolean(false);
  }
  
}

public class ComplexUDFExampleTest {
  
  @Test
  public void testComplexUDFReturnsCorrectValues() throws HiveException {
    
    // set up the models we need
    ComplexUDFExample example = new ComplexUDFExample();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
    JavaBooleanObjectInspector resultInspector = (JavaBooleanObjectInspector) example.initialize(new ObjectInspector[]{listOI, stringOI});
    
    // create the actual UDF arguments
    List<String> list = new ArrayList<String>();
    list.add("a");
    list.add("b");
    list.add("c");
    
    // test our results
    
    // the value exists
    Object result = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("a")});
    Assert.assertEquals(true, resultInspector.get(result));
    
    // the value doesn't exist
    Object result2 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("d")});
    Assert.assertEquals(false, resultInspector.get(result2));
    
    // arguments are null
    Object result3 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(null), new DeferredJavaObject(null)});
    Assert.assertNull(result3);
  }
}
