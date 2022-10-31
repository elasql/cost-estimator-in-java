# Cost Estimator (Java)

This project aims to replicate the model of [our Python estimator](https://github.com/elasql/cost-estimator) in Java. This allows us to port this model to ElaSQL.

## Prerequisite

- Java Development Kit 8+
- Maven

## Assumptions for the Training Data Set

- All values except the following fields can be read as `Double`s
  - `Transaction ID`: Long
  - `Is Master`: Boolean
  - `Is Distributed`: Boolean
  - `Start Time`: Long
- All arrays store features of every server so that we can separate the features for each individual server from the arrays

## Build

```
> mvn package
```

This command generates a file called `estimator-[version number]-jar-with-dependencies.jar`, which includes all the dependencies that this program needs and information for starting the program, in `target` directory.

## Run

### Training

Run the following command to train a cost estimator and saves the corresponding models.

```
> java -jar [Jar File] train [Model Save Dir] [Data Set Dirs]
```

- `[Jar File]`: the path to the built jar file. This is usually `target/estimator-[version number]-jar-with-dependencies.jar`
- `[Model Save Dir]`: the path to the directory for saving the models
- `[Data Set Dirs]`: path(s) to the directories that hold the training data set

Note that this program also needs a configuration file. We assume the configuration file is called `config.toml` and is placed in the working directory. If it is not the case, you can use the following command to specify the path to the file:

```
> java -jar [Jar File] train -c [Config File] [Model Save Dir] [Data Set Dirs]
```

- `[Config File]`: the path to the configuration file. The default is `./config.toml`.

