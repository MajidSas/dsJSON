package edu.ucr.cs.bdlab

object PathProcessor {
  def build(_path_string: String) = {
    var path_string = _path_string
    var tokens = scala.collection.mutable.ArrayBuffer[String]()

    if (path_string.substring(0,1) != "$"){
      throw new Exception("Every path expression must start with $")
    }

    var index = 1;
    path_string = path_string.substring(index)
    while(path_string.length() > 0) {
      // checking for '.' or '..'
      if(path_string.matches("\\.{2}.*")){
        throw new Exception("Descendent elements is currently not supported.")
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

      } else{
        throw new Exception("Invalid JSONPath format: expected to find a key (alphanumeric word) at index " + index.toString())
      }


      // checking for [ and processing array indexes
      if(path_string.matches("\\[.*")){
        tokens.append("[")
        index += 1
        path_string = path_string.substring(1)

        var start_i = "0"
        var end_i = "*"
        if(path_string.matches("\\*\\].*")){ // match all elements
          tokens.append(start_i)
          tokens.append(end_i)
          index += 2
        } else if (path_string.matches("\\d+:\\d+\\].*")){ // match elements i n a range
          val index_range = "(\\d+):(\\d+).*".r
          var index_range(start_i, end_i) = path_string
          index += start_i.length() + end_i.length() + 2
          tokens.append(start_i)
          tokens.append(end_i)
        } else if (path_string.matches("\\d+\\].*")){ // match only one element
          val index_range = "(\\d+).*".r
          var index_range(start_i) = path_string
          index += start_i.length() + 1
          tokens.append(start_i)
          tokens.append((start_i.toInt+1).toString)
        }
      }
      path_string = _path_string.substring(index)
    }
    tokens
  }
}
