import java.io.Serializable
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
//remove if not needed
import scala.collection.JavaConversions._

class KinesisMessageModel extends Serializable {

  var userid: Int = _
  var username: String = _
  var firstname: String = _
  var lastname: String = _
  var city: String = _
  var state: String = _
  var email: String = _
  var phone: String = _
  var likesports: Boolean = _
  var liketheatre: Boolean = _
  var likeconcerts: Boolean = _
  var likejazz: Boolean = _
  var likeclassical: Boolean = _
  var likeopera: Boolean = _
  var likerock: Boolean = _
  var likevegas: Boolean = _
  var likebroadway: Boolean = _s
  var likemusicals: Boolean = _

  def this(userid: Int, 
      username: String, 
      firstname: String, 
      lastname: String, 
      city: String, 
      state: String, 
      email: String, 
      phone: String, 
      likesports: Boolean, 
      liketheatre: Boolean, 
      likeconcerts: Boolean, 
      likejazz: Boolean, 
      likeclassical: Boolean, 
      likeopera: Boolean, 
      likerock: Boolean, 
      likevegas: Boolean, 
      likebroadway: Boolean, 
      likemusicals: Boolean) {
    this()
    this.userid = userid
    this.username = username
    this.firstname = firstname
    this.lastname = lastname
    this.city = city
    this.state = state
    this.email = email
    this.phone = phone
    this.likesports = likesports
    this.liketheatre = liketheatre
    this.likeconcerts = likeconcerts
    this.likejazz = likejazz
    this.likeclassical = likeclassical
    this.likeopera = likeopera
    this.likerock = likerock
    this.likevegas = likevegas
    this.likebroadway = likebroadway
    this.likemusicals = likemusicals
  }

  override def toString(): String = {
    try {
      new ObjectMapper().writeValueAsString(this)
    } catch {
      case e: JsonProcessingException => super.toString
    }
  }

  def getUserid(): Int = userid

  def setUserid(userid: Int) {
    this.userid = userid
  }

  def getUsername(): String = username

  def setUsername(username: String) {
    this.username = username
  }

  def getFirstname(): String = firstname

  def setFirstname(firstname: String) {
    this.firstname = firstname
  }

  def getLastname(): String = lastname

  def setLastname(lastname: String) {
    this.lastname = lastname
  }

  def getCity(): String = city

  def setCity(city: String) {
    this.city = city
  }

  def getState(): String = state

  def setState(state: String) {
    this.state = state
  }

  def getEmail(): String = email

  def setEmail(email: String) {
    this.email = email
  }

  def getPhone(): String = phone

  def setPhone(phone: String) {
    this.phone = phone
  }

  def isLikesports(): Boolean = likesports

  def setLikesports(likesports: Boolean) {
    this.likesports = likesports
  }

  def isLiketheatre(): Boolean = liketheatre

  def setLiketheatre(liketheatre: Boolean) {
    this.liketheatre = liketheatre
  }

  def isLikeconcerts(): Boolean = likeconcerts

  def setLikeconcerts(likeconcerts: Boolean) {
    this.likeconcerts = likeconcerts
  }

  def isLikejazz(): Boolean = likejazz

  def setLikejazz(likejazz: Boolean) {
    this.likejazz = likejazz
  }

  def isLikeclassical(): Boolean = likeclassical

  def setLikeclassical(likeclassical: Boolean) {
    this.likeclassical = likeclassical
  }

  def isLikeopera(): Boolean = likeopera

  def setLikeopera(likeopera: Boolean) {
    this.likeopera = likeopera
  }

  def isLikerock(): Boolean = likerock

  def setLikerock(likerock: Boolean) {
    this.likerock = likerock
  }

  def isLikevegas(): Boolean = likevegas

  def setLikevegas(likevegas: Boolean) {
    this.likevegas = likevegas
  }

  def isLikebroadway(): Boolean = likebroadway

  def setLikebroadway(likebroadway: Boolean) {
    this.likebroadway = likebroadway
  }

  def isLikemusicals(): Boolean = likemusicals

  def setLikemusicals(likemusicals: Boolean) {
    this.likemusicals = likemusicals
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((city == null)) 0 else city.hashCode)
    result = prime * result + (if ((email == null)) 0 else email.hashCode)
    result = prime * result + 
      (if ((firstname == null)) 0 else firstname.hashCode)
    result = prime * result + (if ((lastname == null)) 0 else lastname.hashCode)
    result = prime * result + (if (likebroadway) 1231 else 1237)
    result = prime * result + (if (likeclassical) 1231 else 1237)
    result = prime * result + (if (likeconcerts) 1231 else 1237)
    result = prime * result + (if (likejazz) 1231 else 1237)
    result = prime * result + (if (likemusicals) 1231 else 1237)
    result = prime * result + (if (likeopera) 1231 else 1237)
    result = prime * result + (if (likerock) 1231 else 1237)
    result = prime * result + (if (likesports) 1231 else 1237)
    result = prime * result + (if (liketheatre) 1231 else 1237)
    result = prime * result + (if (likevegas) 1231 else 1237)
    result = prime * result + (if ((phone == null)) 0 else phone.hashCode)
    result = prime * result + (if ((state == null)) 0 else state.hashCode)
    result = prime * result + userid
    result = prime * result + (if ((username == null)) 0 else username.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      return true
    }
    if (obj == null) {
      return false
    }
    if (!(obj.isInstanceOf[KinesisMessageModel])) {
      return false
    }
    val other = obj.asInstanceOf[KinesisMessageModel]
    if (city == null) {
      if (other.city != null) {
        return false
      }
    } else if (city != other.city) {
      return false
    }
    if (email == null) {
      if (other.email != null) {
        return false
      }
    } else if (email != other.email) {
      return false
    }
    if (firstname == null) {
      if (other.firstname != null) {
        return false
      }
    } else if (firstname != other.firstname) {
      return false
    }
    if (lastname == null) {
      if (other.lastname != null) {
        return false
      }
    } else if (lastname != other.lastname) {
      return false
    }
    if (likebroadway != other.likebroadway) {
      return false
    }
    if (likeclassical != other.likeclassical) {
      return false
    }
    if (likeconcerts != other.likeconcerts) {
      return false
    }
    if (likejazz != other.likejazz) {
      return false
    }
    if (likemusicals != other.likemusicals) {
      return false
    }
    if (likeopera != other.likeopera) {
      return false
    }
    if (likerock != other.likerock) {
      return false
    }
    if (likesports != other.likesports) {
      return false
    }
    if (liketheatre != other.liketheatre) {
      return false
    }
    if (likevegas != other.likevegas) {
      return false
    }
    if (phone == null) {
      if (other.phone != null) {
        return false
      }
    } else if (phone != other.phone) {
      return false
    }
    if (state == null) {
      if (other.state != null) {
        return false
      }
    } else if (state != other.state) {
      return false
    }
    if (userid != other.userid) {
      return false
    }
    if (username == null) {
      if (other.username != null) {
        return false
      }
    } else if (username != other.username) {
      return false
    }
    true
  }
}