package kazmongo

import (
	"errors"
	"fmt"

	"github.com/farkaz00/kazstructs"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//MongoClient client to access mongo operations
type MongoClient struct {
	s      *mgo.Session
	dbName string
}

//MongoKey describes the mongodb key
type MongoKey struct {
	Collection string
	Key        []string
	Unique     bool
	DropUps    bool
	Background bool
	Sparse     bool
}

//Select executes Find operation in the collection with the matching selector and returns all matching records
func (mc MongoClient) Select(collection string, selector interface{}, result interface{}) error {
	return mc.Find(collection, selector, result)
}

//SelectOne executes FindOne operation in the collection with the matching selector and returns the first matching record
func (mc MongoClient) SelectOne(collection string, selector interface{}, result interface{}) error {
	return mc.FindOne(collection, selector, result)
}

//Find executes Find operation in the collection with the matching params
func (mc MongoClient) Find(collection string, selector interface{}, result interface{}) error {
	bm, err := structToBson(selector)
	if err != nil {
		return err
	}
	c := mc.s.DB(mc.dbName).C(collection)
	err = c.Find(bm).All(result)

	return err
}

//FindOne executes a FindOne operation in the collection with the matching params
func (mc MongoClient) FindOne(collection string, selector interface{}, result interface{}) error {
	var err error
	m, err := kazstructs.StructToMapLower(selector, true)
	if err != nil {
		return err
	}

	bm := mapToBsonM(m)
	c := mc.s.DB(mc.dbName).C(collection)
	err = c.Find(bm).One(result)
	return err
}

//Insert inserts all data passed as values into the collection
func (mc MongoClient) Insert(collection string, values interface{}) error {
	c := mc.s.DB(mc.dbName).C(collection)
	err := c.Insert(values)
	if mgo.IsDup(err) {
		err = errors.New(fmt.Sprint("The key for this record already exists:", values))
	}
	return err
}

//Update updates all data matching the selector with the values in values
func (mc MongoClient) Update(collection string, selector interface{}, values interface{}) error {
	var err error
	m, err := kazstructs.StructToMapLower(selector, true)
	if err != nil {
		return err
	}

	bm := mapToBsonM(m)
	c := mc.s.DB(mc.dbName).C(collection)

	_, err = c.UpdateAll(bm, bson.M{"$set": &values})

	return err
}

//UpdateOne updates the first record matching the selector with the values in values
func (mc MongoClient) UpdateOne(collection string, selector interface{}, values interface{}) error {
	var err error
	m, err := kazstructs.StructToMapLower(selector, true)
	if err != nil {
		return err
	}

	bm := mapToBsonM(m)
	c := mc.s.DB(mc.dbName).C(collection)

	err = c.Update(bm, bson.M{"$set": &values})

	return err
}

//Delete deletes all records matching criteria in selector
func (mc MongoClient) Delete(collection string, selector interface{}) error {
	bm, err := structToBson(selector)
	if err != nil {
		return err
	}
	c := mc.s.DB(mc.dbName).C(collection)

	_, err = c.RemoveAll(bm)

	return err
}

//DeleteOne deletes a record matching criteria in selector
func (mc MongoClient) DeleteOne(collection string, selector interface{}) error {
	bm, err := structToBson(selector)
	if err != nil {
		return err
	}
	c := mc.s.DB(mc.dbName).C(collection)

	err = c.Remove(bm)

	return err
}

//EnsureIndex ensures indexes
func (mc MongoClient) EnsureIndex(key MongoKey) error {
	c := mc.s.DB(mc.dbName).C(key.Collection)
	index := mgo.Index{
		Key:        key.Key,
		Unique:     key.Unique,
		DropDups:   key.DropUps,
		Background: key.Background,
		Sparse:     key.Sparse,
	}

	err := c.EnsureIndex(index)
	return err
}

//Close closes the underlying Mongo session
func (mc MongoClient) Close() {
	mc.s.Close()
}

//NewMongoClient returns a MongoClient object
func NewMongoClient(conn *MongoConnection, dbName string) (*MongoClient, error) {
	mclient := new(MongoClient)
	mclient.s = conn.Copy()
	mclient.dbName = dbName
	return mclient, nil
}

func structToBson(in interface{}) (bson.M, error) {
	var err error
	m, err := kazstructs.StructToMapLower(in, true)
	if err != nil {
		return nil, err
	}

	bm := mapToBsonM(m)
	return bm, nil
}

func mapToBsonM(m map[string]interface{}) bson.M {
	bm := make(bson.M)

	for k, v := range m {
		switch v.(type) {
		case bool:
			bm[k] = v == true
		case nil:
			bm[k] = nil
		default:
			bm[k] = fmt.Sprintf("%v", v)
		}
	}
	return bm
}
