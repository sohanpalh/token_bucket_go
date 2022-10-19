package main

import (
  "fmt"
  "time"
	"context"
	"github.com/go-co-op/gocron"
  "github.com/go-redis/redis/v8"
)

type TokenBucket struct {
	minuteLimit   int
	hourLimit     int
	dayLimit      int
	redisClient   redis.Cmdable
	scheduler     *gocron.Scheduler
}

func main() {
  scheduler := gocron.NewScheduler(time.UTC)

  // Define redis client as per config
  client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1,
	})

	tokenBucket := TokenBucket{		
		minuteLimit: 1,
		hourLimit:   10,
		dayLimit:    100,
    redisClient: client,
    scheduler:   scheduler,
	}

  // Schedule Crons to fill token buckets
  err := tokenBucket.scheduleTokenFillCron(tokenBucket.dayLimit, "day", "0 0 * * *")
  if err != nil {
    panic(err)
  }

  err = tokenBucket.scheduleTokenFillCron(tokenBucket.hourLimit, "hour", "0 * * * *")
  if err != nil {
    panic(err)
  }

  err = tokenBucket.scheduleTokenFillCron(tokenBucket.minuteLimit, "minute", "* * * * *")
  if err != nil {
    panic(err)
  }

  scheduler.StartAsync()

  tokenBucket.getTokens()
  tokenBucket.getTokens()
  tokenBucket.getTokens()
  tokenBucket.getTokens()
}

func (tb *TokenBucket) scheduleTokenFillCron(limit int, limitType string, cronExp string) error {
  if limit != 0 {
    // Using different REDIS keys for each type of token bucket
    key := fmt.Sprintf("%s_limit_for_process", limitType)
    // Check if the key already exists in REDIS
    limitExists, err := tb.redisClient.Exists(context.Background(), key).Result()
    if err != nil {
      return err
    }
    
    if limitExists == 0 {
      // This is the first time we are running this code, create the key in REDIS with the required tokens
      err = tb.addTokens(limit, limitType)
      if err != nil {
        return err
      }
    }
  }

  _, err := tb.scheduler.Cron(cronExp).Do(tb.addTokens, limit, limitType)

  if err != nil {
    return err
  }

  return nil
}

func (tb *TokenBucket) addTokens(limit int, limitType string) (error) {
  key := fmt.Sprintf("%s_limit_for_process", limitType)
  for i:= 0; i < limit; i++ {
    // Add tokens to this key based on value of i, value of the token does not matter only unique tokens matter
    resp := tb.redisClient.SAdd(context.Background(), key, i)
    if resp.Err() != nil {
      return resp.Err()
    }
  }

  return nil
}

func (tb *TokenBucket) getTokens() {
  fmt.Println("Trying to get day token")
  tb.getToken("day")
  fmt.Println("got day token at ", time.Now().String())
  fmt.Println("Trying to get hour token")
  tb.getToken("hour")
  fmt.Println("got hour token at", time.Now().String())
  fmt.Println("Trying to get minute token")
  tb.getToken("minute")
  fmt.Println("got minute token at", time.Now().String())
}

func (tb *TokenBucket) getToken(limitType string) error {
  done := make(chan string)
  go func() {
    for {
      key := fmt.Sprintf("%s_limit_for_process", limitType)
      storeToken, err := tb.redisClient.SPop(context.Background(), key).Result()

      if err == nil {
        done <- storeToken
        break
      } else {
        tb.waitTillNextTick(limitType)
      }
    }
  }()

  <-done
  return nil
}

func (tb* TokenBucket) waitTillNextTick(limitType string) {
  var cronExp string

  if limitType == "day" {
    cronExp = "0 0 * * *"
  } else if limitType == "hour" {
    cronExp = "0 * * * *"
  } else if limitType == "minute" {
    cronExp = "* * * * *"
  }

  done := make(chan bool)

  // Schedule a cron to run only once to unblock the channel
  tb.scheduler.Cron(cronExp).LimitRunsTo(1).Do(func() {
    done <- true
  })

  <-done
  // Sleeping here to give some time to routine to add tokens
  time.Sleep(5 * time.Second)
}
