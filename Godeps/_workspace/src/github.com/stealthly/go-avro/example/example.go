/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main
import (
    "github.com/stealthly/go-avro"
    "fmt"
)

var schema0 = `{
  "type": "record",
  "name": "Employee",
  "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "emails", "type": {"type": "array", "items": "string"}},
      {"name": "boss", "type": ["Employee","null"]}
  ]
}`

var schema1 = `{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}`

var schema2 = `{
     "type": "record",
     "namespace": "com.example",
     "name": "FullName",
     "fields": [
       { "name": "first", "type": "string" },
       { "name": "last", "type": "string" }
     ]
}`

var schema3 = `{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "age", "type" : "int"}]
} `

var schema4 = `{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "age", "type" : "int", "default" : -1}]
}`

var schema5 = `{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "username",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "age",
                "type" : "int",
                "default" : -1},

                {"name" : "phone",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "housenum",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "street",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "city",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "state_province",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "country",
                "type" : "string",
                "default" : "NONE"},

                {"name" : "zip",
                "type" : "string",
                "default" : "NONE"}]
} `

var schema6 = `{
    "type" : "record",
    "name" : "userInfo",
    "namespace" : "my.example",
    "fields" : [{"name" : "username",
                 "type" : "string",
                 "default" : "NONE"},

                {"name" : "age",
                 "type" : "int",
                 "default" : -1},

                 {"name" : "phone",
                  "type" : "string",
                  "default" : "NONE"},

                 {"name" : "housenum",
                  "type" : "string",
                  "default" : "NONE"},

                  {"name" : "address",
                     "type" : {
                         "type" : "record",
                         "name" : "mailing_address",
                         "fields" : [
                            {"name" : "street",
                             "type" : "string",
                             "default" : "NONE"},

                            {"name" : "city",
                             "type" : "string",
                             "default" : "NONE"},

                            {"name" : "state_prov",
                             "type" : "string",
                             "default" : "NONE"},

                            {"name" : "country",
                             "type" : "string",
                             "default" : "NONE"},

                            {"name" : "zip",
                             "type" : "string",
                             "default" : "NONE"}
                            ]}
                }
    ]
} `

var schema7 = `{ "type" : "enum",
  "name" : "Colors",
  "namespace" : "palette",
  "doc" : "Colors supported by the palette.",
  "symbols" : ["WHITE", "BLUE", "GREEN", "RED", "BLACK"]}`

var schema8 = `{"type" : "array", "items" : "string"}`

var schema9 = `{"type" : "map", "values" : "int"}`

var schema10 = `["string", "null"]`

var schema11 = `{
     "type": "record",
     "namespace": "com.example",
     "name": "FullName",
     "fields": [
       { "name": "first", "type": ["string", "null"] },
       { "name": "last", "type": "string", "default" : "Doe" }
     ]
} `

var schema12 = `{"type" : "fixed" , "name" : "bdata", "size" : 1048576}`

var schema13 = `{
  "type" : "record",
  "name" : "twitter_schema",
  "namespace" : "com.miguno.avro",
  "fields" : [ {
    "name" : "username",
    "type" : "string",
    "doc"  : "Name of the user account on Twitter.com"
  }, {
    "name" : "tweet",
    "type" : "string",
    "doc"  : "The content of the user's Twitter message"
  }, {
    "name" : "timestamp",
    "type" : "long",
    "doc"  : "Unix epoch time in seconds"
  } ],
  "doc:" : "A basic schema for storing Twitter messages"
}`

var schema14 = `{
    "namespace": "com.rishav.avro",
    "type": "record",
    "name": "StudentActivity",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "student_id",
            "type": "int"
        },
        {
            "name": "university_id",
            "type": "int"
        },
        {
            "name": "course_details",
            "type": {
                "name": "Activity",
                "type": "record",
                "fields": [
                    {
                        "name": "course_id",
                        "type": "int"
                    },
                    {
                        "name": "enroll_date",
                        "type": "string"
                    },
                    {
                        "name": "verb",
                        "type": "string"
                    },
                    {
                        "name": "result_score",
                        "type": "double"
                    }
                ]
            }
        }
    ]
}`

func main() {
    sch, err := avro.ParseSchema(schema14)
    if err != nil {
        fmt.Println("ERR:", err)
    }
    fmt.Println(sch)

}
