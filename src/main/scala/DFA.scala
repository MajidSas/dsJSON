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


class State(val stateType: String,
            val value: String,
            var skip: Int, // for array indexing (not used)
            var accept: Int
            // TODO: add value for the size of stack prior to moving to this state
            //       and modify toNextState accordingly
          )
{

  override def toString : String = {
    if(stateType.equals("array")) {
      return "State type: array"
    }

    "State type: %s, Value: %s".format(
      stateType,
      value
      // skip,
      // accept
    )
  }
}

class DFA() {
  var states = scala.collection.mutable.ArrayBuffer[State]()
  var tokens = scala.collection.mutable.ArrayBuffer[(String,Int)]()
  var currentState = 0

  def this(pathTokens: scala.collection.mutable.ArrayBuffer[String]) {
    this()
    var i = 0;
    while (i < pathTokens.length) {
      var token = pathTokens(i)
      if(token.equals("[")) {
        this.states.append(new State("array", ",", 0, 1))
        i+=1
      } else if(token.equals("..")) {
        this.states.append(new State("descendant", pathTokens(i+1), 0, 1))
        i+=2
      } else {
        this.states.append(new State("token", token.toString, 0 , 1))
        this.tokens.append((token.toString, states.size))
        i+=1
      }
    }
  }

  def getStates()  : scala.collection.mutable.ArrayBuffer[State] = {
    states
  }

  def getTokens() : scala.collection.mutable.ArrayBuffer[String] = {
    var _tokens = scala.collection.mutable.ArrayBuffer[String]()
    for(elem <- tokens){
      _tokens.append(elem._1)
    }
    _tokens
  }

  def getTokenStates(token : String) : scala.collection.mutable.ArrayBuffer[Int] = {
    var tokenStates = scala.collection.mutable.ArrayBuffer[Int]()
    for(elem <- tokens){
      if(elem._1.equals(token)){
        tokenStates.append(elem._2)
      }
    }

    tokenStates
  }

  def setState(stateId: Int) {
    currentState = stateId;
  }
  def toNextState() {
    currentState = currentState+1
  }
  def toNextStateIfArray() : Boolean = {
    if(currentState < states.size) {
      if(states(currentState).stateType.equals("array")){
        currentState = currentState+1
        return true
      }
    }
    return false
  }
  def toPrevState() {
    // if(states(currentState).stateType.equals("descendant")) {
    //   if(currentState+1 == level) {
    //     currentState = currentState-1
    //   }
    // }
    // else {
      currentState = currentState-1
    // }
  }
  def getCurrentState() : Int = {
    currentState
  }

  def checkToken(input : String, level : Int) : String = {
    // println((input, level, states(currentState).stateType.equals("descendant"), 
    // input.equals(states(currentState).value), 
    // currentState == states.size))
    if(states(currentState).stateType.equals("descendant")) {
      if(level >= currentState+1 && input.equals(states(currentState).value)) {
        toNextState()
        if(currentState == states.size) {
          currentState -= 1
          return "accept"
        }
      }
      return "continue"
    } else {
      if(level == currentState+1 && input.equals(states(currentState).value)){
        toNextState()
        if(currentState == states.size) {
          currentState -= 1
          return "accept"
        } else {
          return "continue"
        }
      }
      return "reject"
    }
  }

  def checkArray() : String = {
    if(currentState == states.size){
      if(states(currentState-1).stateType.equals("array")) {
        return "accept"
      }
    }
    "continue"
  }

  override def toString = {
    var s = "Current state = " + currentState + "\n"
    var i = 1
    for(state <- states){
      if(i == currentState) {
        s+= "> "
      }
      s += state.toString + "\n"
      i+=1
    }
    s
  }
}