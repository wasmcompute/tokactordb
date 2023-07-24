# Conventually

A database where you declare all of the relationships through code. You program
the business logic and we handle the storage. By understanding how you may use
the database, we can tailor the experience for your needs.

## What makes a database

A database is basically 2 things:

1. Values, Data, Tuples
2. Pointers to the data in a certain order or groupings

The idea for this database is allow for the user to create tables dynamically
using the rust programming language and allow you to execute operations on the
data level rather then constantly coping data between 2 machines.

The goals for this database are the following:

1. Declare the Data that needs to be stored
2. (Aggregate) Declare quires that you want to execute during execution
3. (Index) Declare indexes that you want to be able to keep in sorted order
4. (Select) Declare sub lists that you want to be able to query
