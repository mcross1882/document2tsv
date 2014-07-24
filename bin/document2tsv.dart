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

void main(List<String> argv) {
  if (argv.length < 3) {
    return print("Syntax: document2tsv.dart [mongo address] [collection name] [output file]\n");
  }

  final Db mongo = new Db(argv[0]);
  final DbCollection mongoCollection = mongo.collection(argv[1]);

  final File outputFile = new File(argv[2]);
  final IOSink streamSink = outputFile.openWrite();

  mongo.open().then((_) {
    return writeRowToFile(mongoCollection, streamSink);
  }).then((_) {
    if (null != streamSink) {
      streamSink.close();
    }

    if (null != mongo) {
      mongo.close();
    }

    print("Finished!\n");
  }).catchError((error) {
    print("Caught Exception: ${error.toString()}\n");
  });
}
