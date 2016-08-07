# level-stream-access

Write and read stream values from leveldb database

## Installation

Install _level-stream-access_ from npm. You also need [levelup](https://www.npmjs.com/package/levelup) to be installed.

```
npm install level-stream-access level
```

## Setup

Create a _level-stream-access_ instance by providing a levelup object that is used for storage.

```javascript
const levelup = require('levelup');
const levelStreamAccess = require('level-stream-access');
const levelStream = levelStreamAccess(levelup('./mydb'));
```

## createWriteStream()

Write a large stream into leveldb database

```javascript
let writer = levelStream.createWriteStream('keyname');
```

**Example**

```javascript
fs.createReadStream('file.txt').
    pipe(levelStream.createWriteStream('keyname'));
```

## createReadStream()

Read a stream from leveldb database

```javascript
let reader = levelStream.createReadStream('keyname');
```

**Example**

```javascript
levelStream.createReadStream('keyname').
    pipe(process.stdout);
```

## createStoreStream()

Write a large stream into leveldb database and immediatelly read the stored data. Use it if you want to pass on data but you need to make sure that data gets stored.

```javascript
let store = levelStream.createStoreStream('keyname');
```

**Example**

```javascript
fs.createReadStream('file.txt').
    pipe(levelStream.createStoreStream('keyname')).
    pipe(process.stdout);
```

## prepend()

Prepend data to the beginning of the stored stream. This is mostly useful if the stream is an email
message and you want to add new headers to it

```javascript
levelStream.prepend(keyname, value, callback);
```

Where

- **keyname** is the stream key to prepend data to
- **value** is a Buffer or string data to be prepended to the stream
- **callback** is the function to run once data is prepended

**Example**

```javascript
levelStream.delete('keyname', 'new first line\n' function(err, prepended){
    if(err){
        console.log(err);
    }else if(!prepended){
        console.log('Stream was not found');
    }else{
        console.log('data prepended to the stream');
    }
});
```

## delete()

Delete streamed data from leveldb

```javascript
levelStream.delete('keyname', callback);
```

**Example**

```javascript
levelStream.delete('keyname', function(err, deleted){
    if(err){
        console.log(err);
    }else{
        console.log('%s chunks deleted', deleted);
    }
});
```

## License

**MIT**
