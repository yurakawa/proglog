package main

import "fmt"

type Config struct {
	Type string
}

func (c *Config) SetType(t string) {
	fmt.Println("hoge", c)
	fmt.Println(t)

}

func main() {
	c := &Config{}
	c = nil
	c.SetType("hoge")

}
