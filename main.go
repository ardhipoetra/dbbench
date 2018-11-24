package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sj14/dbbench/databases"
)

// Bencher is the interface a benchmark has to impelement.
type Bencher interface {
	Setup(...string)
	Cleanup()
	Benchmarks() []databases.Benchmark
	Exec(string)
}

func main() {
	var (
		version = "version undefined"

		dbType = flag.String("type", "", "database to use (sqlite|mariadb|mysql|postgres|cockroach|cassandra|scylla)")
		host   = flag.String("host", "localhost", "address of the server")
		port   = flag.Int("port", 0, "port of the server")
		user   = flag.String("user", "root", "user name to connect with the server")
		pass   = flag.String("pass", "root", "password to connect with the server")
		// dbName = flag.String("db", "dbbench", "database created for the benchmark")
		// path   = flag.String("path", "dbbench.sqlite", "database file (sqlite only)")
		conns       = flag.Int("conns", 0, "max. number of open connections")
		iter        = flag.Int("iter", 1000, "how many iterations should be run")
		threads     = flag.Int("threads", 25, "max. number of green threads")
		clean       = flag.Bool("clean", false, "only cleanup benchmark data, e.g. after a crash")
		noclean     = flag.Bool("noclean", false, "keep benchmark data")
		versionFlag = flag.Bool("version", false, "print version information")
		runBench    = flag.String("run", "all", "only run the specified benchmarks, e.g. \"inserts deletes\"")
		scriptname  = flag.String("script", "", "custom sql file to execute")
	)

	flag.Parse()

	if *versionFlag {
		fmt.Printf("dbbench %s\n", version)
		os.Exit(0)
	}

	bencher := getImpl(*dbType, *host, *port, *user, *pass, *conns)

	// only clean old data when clean flag is set
	if *clean {
		bencher.Cleanup()
		os.Exit(0)
	}

	// setup database
	bencher.Setup()

	// only cleanup benchmark data when noclean flag is not set
	if !*noclean {
		defer bencher.Cleanup()
	}

	// split benchmark names when "-run 'bench0 bench1 ...'" flag was used
	toRun := strings.Split(*runBench, " ")

	start := time.Now()
	for _, r := range toRun {
		benchmark(bencher, *scriptname, r, *iter, *threads)
	}
	fmt.Printf("total: %v\n", time.Since(start))
}

func getImpl(dbType string, host string, port int, user, password string, maxOpenConns int) Bencher {
	switch dbType {
	case "sqlite":
		if maxOpenConns != 0 {
			log.Fatalln("can't use 'conns' with SQLite")
		}
		return databases.NewSQLite()
	case "mysql", "mariadb":
		return databases.NewMySQL(host, port, user, password, maxOpenConns)
	case "postgres":
		return databases.NewPostgres(host, port, user, password, maxOpenConns)
	case "cockroach":
		return databases.NewCockroach(host, port, user, password, maxOpenConns)
	case "cassandra", "scylla":
		if maxOpenConns != 0 {
			log.Fatalln("can't use 'conns' with Cassandra or ScyllaDB")
		}
		return databases.NewCassandra(host, port, user, password)
	}

	log.Fatalln("missing or unknown type parameter")
	return nil
}

func benchmark(bencher Bencher, filename, runBench string, iterations, goroutines int) {
	wg := &sync.WaitGroup{}
	// w := tabwriter.NewWriter(os.Stdout, 0, 4, 4, '\t', tabwriter.AlignRight)

	if filename != "" {
		lines, err := readLines(filename)
		if err != nil {
			log.Fatalf("failed to read file: %v", err)
		}
		wg.Add(goroutines)
		start := time.Now()

		script := ""

		// store statements in a single line, to execute them at once,
		// otherwise it would cause race conditions with the database and the goroutines
		for _, l := range lines {
			script += l
		}

		execScript(wg, bencher, script, iterations, goroutines)
		wg.Wait()
		elapsed := time.Since(start)
		fmt.Printf("custom script: %v\t%v\tns/op\n", elapsed, elapsed.Nanoseconds()/int64(iterations))
		return
	}

	for _, b := range bencher.Benchmarks() {
		// check if we want to run this particular benchmark
		if runBench != "all" && b.Name != runBench {
			continue
		}

		wg.Add(goroutines)
		start := time.Now()
		execBenchmark(wg, b, iterations, goroutines)
		wg.Wait()
		elapsed := time.Since(start)
		fmt.Printf("%v\t%v\t%v\tns/op\n", b.Name, elapsed, elapsed.Nanoseconds()/int64(iterations))

		// fmt.Fprintf(w, "%v\t%v\t%v\tns/op\n", name, elapsed, elapsed.Nanoseconds()/int64(iterations))
	}
	// w.Flush()
}

func execScript(wg *sync.WaitGroup, bencher Bencher, script string, iterations, goroutines int) {
	for i := 0; i < goroutines; i++ {
		from := (iterations / goroutines) * i
		to := (iterations / goroutines) * (i + 1)

		go func() {
			defer wg.Done()
			for i := from; i < to; i++ {
				bencher.Exec(script)
			}
		}()
	}
}

func execBenchmark(wg *sync.WaitGroup, b databases.Benchmark, iterations, goroutines int) {
	for i := 0; i < goroutines; i++ {
		from := (iterations / goroutines) * i
		to := (iterations / goroutines) * (i + 1)

		go func() {
			defer wg.Done()

			switch b.Type {
			case databases.Single:
				b.Func(1)
			case databases.Loop:
				for i := from; i < to; i++ {
					b.Func(i)
				}
			}
		}()
	}
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
