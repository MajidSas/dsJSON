#include <iostream>
#include "simdjson.h"
using namespace simdjson;
int main(void) {
    ondemand::parser parser;
    padded_string json = padded_string::load("/Users/majid/Documents/json-stream/input/bestbuy_large_record.json");
    auto document = parser.iterate(json);
    auto products = document.find_field("products").get_array();
    for (ondemand::object product : products) {
        auto categoryPath = product.find_field("categoryPath").get_array();
        for(ondemand::object category : categoryPath) {
           for (auto field : category) {
              field.value();
           }
        }
    }
}
