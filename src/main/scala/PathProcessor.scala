/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucr.cs.bdlab

object PathProcessor {
  def findMatchingBracket(str : String, isSquare : Boolean = false) : String = {
    var counter = 1 // assuming str starts from after the first open bracket
    val openBracket = if(isSquare) { '[' } else { '(' }
    val closeBracket = if(isSquare) { ']' } else { ')' }
    var i = 0
    while(i < str.size) {
      if(str(i) == openBracket) {
        counter += 1
      } else if(str(i) == closeBracket) {
        counter -= 1
        if(counter == 0) {
          return str.substring(0, i)
        }
      }

      i+=1
    }
    return str
  }
  def build(_path_string: String) = {
    var path_string = _path_string
    var tokens = scala.collection.mutable.ArrayBuffer[String]()
    var pathFilter = scala.collection.mutable.ArrayBuffer[String]()
    if (path_string.substring(0,1) != "$"){
      throw new Exception("Every path expression must start with $")
    }
    var index = 1;
    path_string = path_string.substring(index)
    while(path_string.length() > 0) {
      // checking for '.' or '..'
      if(path_string.matches("\\.{2}.*")){
        index += 2
        tokens.append("..")
        path_string = path_string.substring(2)
      }
      else if(path_string.matches("\\..*")){
        index += 1
        path_string = path_string.substring(1)
      } else {
        throw new Exception("Invalid JSONPath format: looking for '.' or '..' at index " + index.toString())
      }
      // checking for keys alphanumeric strings in this case
      if(path_string.matches("\\w+.*")){
        val word = "(\\w+).*".r
        val word(matched_word) = path_string
        index += matched_word.length()
        tokens.append(matched_word)
        path_string = path_string.substring(matched_word.length())
      } 
      // checking for [ and processing array indexes
      if(path_string.matches("\\[.*")){
        tokens.append("[")
        index += 1
        path_string = path_string.substring(1)
        if(path_string.trim.startsWith("?(")) {
          val filterIndex = path_string.indexOf("?(")
          val filterString = findMatchingBracket(path_string.substring(filterIndex+2))
          pathFilter.append(filterString)
          path_string = path_string.substring(filterIndex+2+filterString.size+1)
          
          index += filterIndex+2+filterString.size+1+path_string.indexOf("]")+1
        }
        else if(path_string.trim.startsWith("*]")) {
          index += path_string.indexOf("*]")+2
        }
        // indexing is not supported
        // var start_i = "0"
        // var end_i = "*"
        // if(path_string.matches("\\*\\].*")){ // match all elements
        //   tokens.append(start_i)
        //   tokens.append(end_i)
        //   index += 2
        // } else if (path_string.matches("\\d+:\\d+\\].*")){ // match elements i n a range
        //   val index_range = "(\\d+):(\\d+).*".r
        //   var index_range(start_i, end_i) = path_string
        //   index += start_i.length() + end_i.length() + 2
        //   tokens.append(start_i)
        //   tokens.append(end_i)
        // } else if (path_string.matches("\\d+\\].*")){ // match only one element
        //   val index_range = "(\\d+).*".r
        //   var index_range(start_i) = path_string
        //   index += start_i.length() + 1
        //   tokens.append(start_i)
        //   tokens.append((start_i.toInt+1).toString)
        // }
      }
      // TODO: check if object key has filter between two brackets, and extract it
      path_string = _path_string.substring(index)
    }
    (tokens, pathFilter)
  }
}
