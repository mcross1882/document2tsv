/**
 * document2tsv.dart
 *
 * Converts mongodb collections into TSV files. This script
 * assumes your documents share the same structure as the first
 * document in the collection.
 *
 * @author  Matthew Cross <matthew@pmg.co>
 * @pacakge document2tsv
 * @license Apache V2
 */
import 'dart:io';
import 'dart:async';
import 'package:mongo_dart/mongo_dart.dart';

void main(List<String> arguments) {
  if (arguments.length < 3) {
    return print("Syntax: document2tsv.dart [mongo address] [collection name] [output file]\n");
  }

  try {
    copyCollectionToFile(arguments);
  } catch(error) {
    print("Caught Exception: ${error.toString()}\n");
  }
}

void copyCollectionToFile(List<String> arguments) {
  final Db mongo = new Db(arguments[0]);
  final DbCollection mongoCollection = mongo.collection(arguments[1]);

  final File outputFile = new File(arguments[2]);
  final IOSink streamSink = outputFile.openWrite();

  mongo.open().then((_) {
    return writeRowToFile(mongoCollection, streamSink);
  }).then((_) {
    streamSink.close();
    mongo.close();

    print("Finished!\n");
  }).catchError((error) {
    print("Caught Exception: ${error.toString()}\n");
  });
}

Future<bool> writeRowToFile(DbCollection collection, IOSink streamSink) {
  var cursor = collection.find();

  return cursor.nextObject().then((Map document) {
    streamSink
      ..writeln(document.keys.join("\t"))
      ..writeln(document.values.join("\t"));
  })
  .then((_) {
    return cursor.forEach((Map document) {
      streamSink.writeln(document.values.join("\t"));
    });
  });
}
