package misc

import "os"

func IsFileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}
