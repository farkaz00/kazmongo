package kazmongo

import (
	"log"

	"github.com/farkaz00/kazconfig"
	mgo "gopkg.in/mgo.v2"
)

//MongoConnection encapsulates a Mongo connection
type MongoConnection struct {
	mSession *mgo.Session
}

//GetConnString returns a MySQL formatted connection string
func (conn MongoConnection) GetConnString() string {
	var strConn string
	strConn = ""
	return strConn
}

//Close closes the MySQL connection
func (conn MongoConnection) Close() {

}

//Copy wrapper for the internal Mongo session object
func (conn MongoConnection) Copy() *mgo.Session {
	return conn.mSession.Copy()
}

//NewMongoConnection returns a MongoConnection object
func NewMongoConnection(s *kazconfig.Settings) *MongoConnection {
	var err error
	var tempMongos *mgo.Session

	tempMongos, err = mgo.Dial(s.Get("dbhost"))
	if err != nil {
		log.Print(err)
	}

	tempMongos.SetMode(mgo.Monotonic, true)

	if err := tempMongos.DB(s.Get("dbname")).Login(s.Get("dbuser"), s.Get("dbpwd")); err != nil {
		log.Print(err)
	}

	conn := &MongoConnection{
		mSession: tempMongos,
	}

	return conn
}
