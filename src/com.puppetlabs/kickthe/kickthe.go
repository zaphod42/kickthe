package main

import "fmt"
import "os"
import "com.puppetlabs/kickthe/bucket"

func main() {
  action := os.Args[1]

  switch action {
  case "save":
    base := os.Args[2]
    file := os.Args[3]

    system := bucket.NewBucketSystem(base)

    input, err := os.Open(file)
    if err != nil {
      panic(err)
    }

    bucket, err := system.Save(input, file)
    if err != nil {
      panic(err)
    }
    
    fmt.Printf("Saved %d bytes\n", bucket.Size())
  case "check":
    base := os.Args[2]
    digest := os.Args[3]

    system := bucket.NewBucketSystem(base)

    if system.Check(digest) {
      fmt.Printf("%s is in the buckets\n", digest)
    } else {
      fmt.Printf("%s is NOT in the buckets\n", digest)
    }
  case "help":
    fmt.Printf("kickthe command bucketlocation arg\n")
    fmt.Printf("\n")
    fmt.Printf("Commands:\n")
    fmt.Printf("\tsave  - save a file to the bucketlocation. arg is the path to the file\n")
    fmt.Printf("\tcheck - check if a digest is stored in the bucketlocation. arg is the digest\n")
    fmt.Printf("\thelp  - this\n\n")
  }
}
