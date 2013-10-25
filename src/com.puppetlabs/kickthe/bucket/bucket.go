package bucket

import (
  "strings"
  "fmt"
  "os"
  "io/ioutil"
  "io"
  "crypto/sha1"
  "path/filepath"
)

type BucketSystem struct {
  data *BucketBrigade
  names *BucketBrigade
}

type BucketBrigade struct {
  base string
}

type Bucket struct {
  hash string
  location string
  size int64
}

func NewBucketSystem(location string) *BucketSystem {
  return &BucketSystem{NewBucketBrigade(filepath.Join(location, "data")), NewBucketBrigade(filepath.Join(location, "names"))}
}

func NewBucketBrigade(location string) *BucketBrigade {
  os.Mkdir(location, 0700)
  return &BucketBrigade{location}
}

func (self *BucketSystem) Check(digest string) bool {
  return self.data.Check(digest)
}

func (self *BucketBrigade) Check(digest string) bool {
  _, err := os.Stat(self.PathTo(digest))
  return err == nil
}

func (self *BucketSystem) Save(data io.Reader, name string) (bucket *Bucket, err error) {
  b, err := self.data.Save(data)
  if err == nil {
    self.names.Save(strings.NewReader(strings.Join([]string{b.hash, name}, ":"))) 
  }
  return b, err
}

func (self *BucketBrigade) Save(data io.Reader) (bucket *Bucket, err error) {
  file, err := ioutil.TempFile("", "tempbucket")
  if err != nil {
    return nil, err
  }

  hash := sha1.New()

  splay := io.MultiWriter(file, hash)

  copied, err := io.Copy(splay, data)
  if err != nil {
    file.Close()
    os.Remove(file.Name())
    return nil, err
  }

  err = file.Sync()
  if err != nil {
    file.Close()
    os.Remove(file.Name())
    return nil, err
  }

  digest := fmt.Sprintf("%x", hash.Sum(nil))

  file.Close()
  path := self.PathTo(digest)
  err = os.Rename(file.Name(), path)
  if err != nil {
    return nil, err
  }

  return &Bucket{location: path, hash: digest, size: copied}, nil
}

func (self *BucketBrigade) PathTo(digest string) string {
  parts := strings.SplitN(digest, "", 7)
  directory := filepath.Join(self.base, filepath.Join(parts[0:6]...))
  os.MkdirAll(directory, 0700)
  return filepath.Join(directory, digest)
}

func (self *Bucket) Size() int64 {
  return self.size
}
