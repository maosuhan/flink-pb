# Flink pb serialization format

## Environment
1. java8
2. maven
3. proto installed in /usr/local/bin/protoc. test successfully with the version >= 3.12.2.
4. test successfully with protobuf-java>=3.12.2

## Run 
Run `mvn clean install`

Run a complete example: `org.apache.flink.pb.starter.Main`

## Connector params
* pb.ignore-parse-errors: default is false. Task can continue running when pb format parse error.

* pb.ignore-default-values: default is false. If user ignore the default value, the row field value must be null 
if the pb field value is not set regardless of if it has a default value. When this setting is true, the row field value
will always return a non-null value.

    For example of proto2 syntax:
    
        message SimpleTest {
            optional int32 a = 1 [default=10];
            optional int64 b = 2 [default=100];
        }
            
    pb.ignore-default-values=true
    
    `SimpleTest.newBuilder().setA(88).build()`, it will generate row(88, null)
    
    pb.ignore-default-values=false
    
    `SimpleTest.newBuilder().setA(88).build()`, it will generate row(88, 100) while 100 is the default value of `b`

    If the message class is proto3 syntax, pb.ignore-default-values will always be false.

* pb.message-class-name: The full java class name of proto class. 

## Attention
* The data type in table schema must be exactly match the pb java type. Below is the mapping:

|  JavaType   | LogicalType  |
|  ----  | ----  |
| STRING  | VARCHAR or CHAR |
| ENUM  | VARCHAR or CHAR |
| BOOLEAN  |BOOLEAN |
| BYTE_STRING  |BINARY |
| INT  | INT |
| LONG  | BIGINT |
| FLOAT  | FLOAT |
| DOUBLE  | DOUBLE |
| MESSAGE  | ROW |
| field descriptor is repeated type  | ARRAY |
| field descriptor is map type  | Map |

* The order and the number of columns can be defined freely.


* If the output pb format has `one-of` field, flink serializer will overwrite the field by the value of largest position in result row schema.

* In proto3 format, default protobuf serializer will not set field value if the value is equals to pb's default value of each type. For example, int -> 0, long -> 0L, string -> "".
But flink serializer will output all the non-null values to pb bytes regardless of if the value is equals to pb's default value.

## Future Plan
* time attribute column

* add timestamp support