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
* pb.ignore-parse-errors: default is false. Deserialization task will continue running ignoring pb parse errors.

* pb.ignore-default-values: default is false. In deserialization process, if user ignore the default value, the nullability of row field value only depends
on if this pb field value is set explicitly by API regardless of whether it has a default value or not. When this setting is false, the row field value
is always a non-null value mixing default value.

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

## Notice
* The data type in table schema must be exactly match the pb java type. Below is the mapping:

|  Pb JavaType   | LogicalType  |
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
| REPEATED  | ARRAY |
| MAP  | Map |

* The order and the number of columns can be defined freely.

* If the output pb format has `one-of` field, the value is determined by the biggest position among the candidate non-null elements in result row schema.

* In proto3 format, default protobuf serializer will not set field value if the value is equals to pb's default value of each type. For example, int -> 0, long -> 0L, string -> "".
But this serializer will output all the non-null values to pb bytes regardless of if the value is equals to pb's default value.

* This serializer will not sort map keys when serialize the map type to bytes. 
So the output byte array of two equal map may differ because of the key order issue, but it will not affect pb consumers in normal case.

## Null Value

In serialization process, flink row may contain null value in row/map/array.
But protobuf treat null value differently, we should take care of it.

If flink row contains null element, this serializer will not write this field in the protobuf stream.

If downstream user read this stream:

1. With proto2 class, user can call proto.hasXXX() method to know if this field exists in the stream.

2. With proto3 class, proto.hasXXX() is deprecated, so there is no way to know whether this field does not exist or is coincidentally set with a default value.


The default value of each type is defined below:

| pb type | default value |
| ---- | ---- |
| boolean |  false |
| int | 0 |
| long | 0L |
| float | 0.0f |
| double | 0.0 |
| string | "" |
| enum | first enum value |
| binary | empty byte array |
| list | empty list |
| map | empty map |
| message | empty message |

Exmaple null conversion:

| row value | pb value |
| ---- | ---- |
| map<string,string>(<"a", null>)  | map<string,string>(("a", "")) |
| map<string,string>(<null, "a">)  | map<string,string>(("", "a")) |
| map<int, int>(null, 1) | map<int, int>(0, 1) |
| map<int, int>(1, null) | map<int, int>(1, 0) |
| map<long, long>(null, 1) | map<long, long>(0, 1) |
| map<long, long>(1, null) | map<long, long>(1, 0) |
| map<bool, bool>(null, true) | map<bool, bool>(false, true) |
| map<bool, bool>(true, null) | map<bool, bool>(true, false) |
| map<string, enum>("key", null) | map<string, enum>("key", <first enum value type>) |
| map<string, binary>("key", null) | map<string, binary>("key", BytrString.EMPTY) |
| map<string, MESSAGE>("key", null) | map<string, MESSAGE>("key", MESSAGE.getDefaultInstance()) |
| array<>(null) | array<>() |

