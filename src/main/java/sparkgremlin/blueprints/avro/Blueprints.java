/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package sparkgremlin.blueprints.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface Blueprints {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"Blueprints\",\"namespace\":\"sparkgremlin.blueprints.avro\",\"types\":[{\"type\":\"record\",\"name\":\"Property\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":[\"long\",\"string\"]}]},{\"type\":\"record\",\"name\":\"AvroEdge\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"src\",\"type\":\"long\"},{\"name\":\"dest\",\"type\":\"long\"},{\"name\":\"props\",\"type\":{\"type\":\"array\",\"items\":\"Property\"}}]},{\"type\":\"record\",\"name\":\"AvroVertex\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"props\",\"type\":{\"type\":\"array\",\"items\":\"Property\"}}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  public interface Callback extends Blueprints {
    public static final org.apache.avro.Protocol PROTOCOL = sparkgremlin.blueprints.avro.Blueprints.PROTOCOL;
  }
}