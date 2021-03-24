package databases

import (
	"database/sql"
	"fmt"
	"log"
	"context"

	"github.com/sj14/dbbench/benchmark"
	"github.com/ardhipoetra/go-dqlite/client"
	"github.com/ardhipoetra/go-dqlite/driver"
	"github.com/ardhipoetra/go-dqlite/protocol"
)

var leader_cli *client.Client
var voter_cli []*client.Client

var leadercli *client.Client

var (
	dbPath    string
	leaderu   string
	voteru 	[]string
	dbCreated bool // DB file was created by dbbench
)

func connectClients() {
	// connect each client with dqlite instance
	leader_cli, _ = client.New(context.Background(), leaderu)
	log.Printf("Leader: %v\n", leaderu)
	for i, v := range voteru {
		log.Printf("Voter %d: %v\n", i, v)
		voter_cli[i], _ = client.New(context.Background(), v)
	}
}

func setupCluster() protocol.NodeStore {
	// set the inmemnodestore to refer the cluster
	store := client.NewInmemNodeStore()
	store.Set(context.Background(), []client.NodeInfo{{Address: leaderu}})

	log.Printf("Find leader...")
	leadercli, _ := client.FindLeader(context.Background(), store, []client.Option{client.WithDialFunc(client.DefaultDialFunc)}...)
	log.Printf("Leader is: %s", leadercli)

	for i, v := range voteru {
		// prepare node 2 and 3 to be added to the leader
		// the leader by default has ID = 1 or BootstrapID (some hardcoded value)
		client_voter := client.NodeInfo{ID: uint64(i)+uint64(2), Address: v, Role: client.Voter}

		// add node2
		log.Printf("(%d) Add Client %s ...", client_voter.ID, client_voter)
		err := leadercli.Add(context.Background(), client_voter)
		if err != nil {
			fmt.Errorf("Cannot add node %s %s\n", client_voter, err)
		}
	}

	return store
}

type DQLite struct {
	db *sql.DB
}

func NewDQLite(path string, leader_ string, voter_ []string) *DQLite {
	dbPath = path
	leaderu = leader_
	voteru = voter_
	voter_cli = make([]*client.Client, len(voter_))

	// connect clients so we can use later
	log.Printf("Connect to client...")
	connectClients()

	// setup the cluster
	log.Printf("Setup the cluster...")
	store := setupCluster()
	driver, _ := driver.New(store)
	sql.Register("dqlite", driver)

	// if _, err := os.Stat(path); os.IsNotExist(err) {
	// 	// We will create the database file.
	// 	dbCreated = true
	// }

	// Automatically creates the DB file if it doesn't exist yet.
	db, err := sql.Open("dqlite", "dqlite_benchmark")
	if err != nil {
		log.Fatalf("failed to open connection: %v\n", err)
	}

	db.SetMaxOpenConns(1)
	p := &DQLite{db: db}

	printCluster()

	return p
}

func (m *DQLite) Benchmarks() []benchmark.Benchmark {
	log.Printf("Call benchmarks()...")
	return []benchmark.Benchmark{
		{Name: "inserts", Type: benchmark.TypeLoop, Stmt: "INSERT INTO dbbench_simple (id, balance) VALUES( {{.Iter}}, {{call .RandInt63}});"},
		{Name: "selects", Type: benchmark.TypeLoop, Stmt: "SELECT * FROM dbbench_simple WHERE id = {{.Iter}};"},
		{Name: "updates", Type: benchmark.TypeLoop, Stmt: "UPDATE dbbench_simple SET balance = {{call .RandInt63}} WHERE id = {{.Iter}};"},
		{Name: "deletes", Type: benchmark.TypeLoop, Stmt: "DELETE FROM dbbench_simple WHERE id = {{.Iter}};"},
		// {"relation_insert0", benchmark.TypeLoop, "INSERT INTO dbbench_relational_one (oid, balance_one) VALUES( {{.Iter}}, {{call .RandInt63}});"},
		// {"relation_insert1", benchmark.TypeLoop, "INSERT INTO dbbench_relational_two (relation, balance_two) VALUES( {{.Iter}}, {{call .RandInt63}});"},
		// {"relation_select", benchmark.TypeLoop, "SELECT * FROM dbbench_relational_two INNER JOIN dbbench_relational_one ON dbbench_relational_one.oid = relation WHERE relation = {{.Iter}};"},
		// {"relation_delete1", benchmark.TypeLoop, "DELETE FROM dbbench_relational_two WHERE relation = {{.Iter}};"},
		// {"relation_delete0", benchmark.TypeLoop, "DELETE FROM dbbench_relational_one WHERE oid = {{.Iter}};"},
	}
}

// Setup initializes the database for the benchmark.
func (m *DQLite) Setup() {
	log.Printf("Do setup...")
	if _, err := m.db.Exec("CREATE TABLE IF NOT EXISTS dbbench_simple (id INT PRIMARY KEY, balance DECIMAL);"); err != nil {
		log.Fatalf("failed to create table dbbench_simple: %v\n", err)
	}
	if _, err := m.db.Exec("CREATE TABLE IF NOT EXISTS dbbench_relational_one (oid INT PRIMARY KEY, balance_one DECIMAL);"); err != nil {
		log.Fatalf("failed to create table dbbench_relational_one: %v\n", err)
	}
	if _, err := m.db.Exec("CREATE TABLE IF NOT EXISTS dbbench_relational_two (balance_two DECIMAL, relation INT PRIMARY KEY, FOREIGN KEY(relation) REFERENCES dbbench_relational_one(oid));"); err != nil {
		log.Fatalf("failed to create table dbbench_relational_two: %v\n", err)
	}
	if _, err := m.db.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		log.Fatalf("failed to enabled foreign keys: %v\n", err)
	}
}

// Cleanup removes all remaining benchmarking data.
func (m *DQLite) Cleanup() {
	if _, err := m.db.Exec("DROP TABLE dbbench_simple"); err != nil {
		log.Printf("failed to drop table: %v\n", err)
	}
	if _, err := m.db.Exec("DROP TABLE dbbench_relational_two"); err != nil {
		log.Printf("failed to drop table: %v\n", err)
	}
	if _, err := m.db.Exec("DROP TABLE dbbench_relational_one"); err != nil {
		log.Printf("failed to drop table: %v\n", err)
	}
	if err := m.db.Close(); err != nil {
		log.Printf("failed to close connection: %v", err)
	}

	// The DB file existed before, don't remove it.
	// if !dbCreated {
	// 	return
	// }

	// if err := os.Remove(dbPath); err != nil {
	// 	log.Printf("not able to delete created database file: %v\n", err)
	// }
}

// Exec executes the given statement on the database.
func (m *DQLite) Exec(stmt string) {
	//  driver has no support for results
	_, err := m.db.Exec(stmt)
	if err != nil {
		log.Printf("%v failed: %v", stmt, err)
	}
}

func printCluster() {
	var leader_ni *protocol.NodeInfo
	log.Printf("Printing cluster..")

	if leader_cli != nil {
		log.Println("From Leader:")
		leader_ni, _ = leader_cli.Leader(context.Background())
		log.Println(leader_ni.ID, " at ", leader_ni.Address)
		servers, _ := leader_cli.Cluster(context.Background())
		for _, ni := range servers {
			fmt.Printf("%s--%s,", ni.Address, ni.Role)
		}
		fmt.Println("\n-----------------")
	}

	for i, v := range voter_cli {
		log.Printf("(%d) From Node %s:", i, v)
		leader_ni, _ = v.Leader(context.Background())
		log.Println("My leader is : ", leader_ni.ID, " at ", leader_ni.Address)
		servers, _ := v.Cluster(context.Background())
		for _, ni := range servers {
			fmt.Printf("%s--%s,", ni.Address, ni.Role)
		}
		fmt.Println("\n-----------------")
	}
}