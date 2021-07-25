package edu.ucr.cs.bdlab


class State(val stateType: String,
            val value: String,
            var skip: Int,
            var accept: Int)
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
        val skip = pathTokens(i+1).toInt
        var accept = 1
        if(pathTokens(i+2).equals("*")){
          accept = -1
        } else {
          accept = pathTokens(i+2).toInt - skip
        }
        // this.states.append(new State("array", "[", skip, accept))
        this.states.append(new State("array", ",", skip, accept))
        i=i+3
      } else {
        this.states.append(new State("token", token.toString, 0 , 1))
        this.tokens.append((token.toString, states.size))
        i=i+1
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
    // println("updateState", currentState)
  }
  def toNextStateIfArray() : Boolean = {
    if(currentState < states.size) {
      if(states(currentState).stateType.equals("array")){
        currentState = currentState+1
        // println("updateStateArray", currentState)
        return true
      }
    }
    return false
  }
  def toPrevState() {
    currentState = currentState-1
  }
  def getCurrentState() : Int = {
    currentState
  }

  def checkToken(input : String, level : Int) : String = {
    // println(input, states(currentState-1).value)
    if(level == currentState+1 && input.equals(states(currentState).value)){
      toNextState()
      if(currentState == states.size) {
        return "accept"
      } else {
        return "continue"
      }
    }
    return "reject"
  }

  def checkArray() : String = {
    // println("checkArray", currentState, states.size, states(currentState-1).stateType)
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