package kazmongo

import (
	"fmt"
	"strings"
)

//GetConnectionString Build connection string for mongodb
func getConnectionString(host string,
	port string,
	db string,
	usr string,
	pwd string) string {

	var mongostr string

	mongostr += `mongodb://` + strings.TrimSpace(usr) + `:` + strings.TrimSpace(pwd) + "@" + strings.TrimSpace(host)
	if strings.TrimSpace(port) != "" {
		mongostr += ":" + strings.TrimSpace(port)
	}
	mongostr += "/" + db

	fmt.Println(mongostr)

	return mongostr
}
