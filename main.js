const path = require('node:path');
const avro = require('avsc');
const assert = require('assert');
const data = require('./info');
const axios = require('axios');

class DateType extends avro.types.LogicalType {
    constructor(schema, opts) {
        super(schema, opts);
        if (!avro.Type.isType(this.getUnderlyingType(), 'long', 'string')) {
            throw new Error('invalid underlying date type');
        }
    }

    _fromValue(val) { return new Date(val); }

    _toValue(date) {
        if (!(date instanceof Date)) {
            return undefined;
        }
        if (this.getUnderlyingType().typeName === 'long') {
            return +date;
        } else {
            // String.
            return '' + date;
        }
    }

    _resolve(type) {
        if (avro.Type.isType(type, 'long', 'string', 'logical:local-timestamp-millis')) {
            return this._fromValue;
        }
    }
}

function getDataTime(date) {
    const denom = 10;
    const roundedDate = new Date(date);
    const roundedMinutes = Math.floor(roundedDate.getMinutes() / denom) * denom;
    roundedDate.setMinutes(roundedMinutes);
    roundedDate.setSeconds(0);
    roundedDate.setMilliseconds(0);
    return roundedDate;
}

async function main() {
    let cur_date_time = getDataTime(new Date());
    let logicalTypeDefs = { 'local-timestamp-millis': DateType };
    let schemaPath = path.resolve('./ParkingLotStatus.avsc');
    let schema = avro.parse(schemaPath, { logicalTypes: logicalTypeDefs });

    let encoder = new avro.streams.BlockEncoder(schema);

    data.info.forEach((item) => {
        item.id = Number(item.id);
        item.date_time = cur_date_time;
        encoder.write(item);
    });
    encoder.end();

    const buff = [];
    for await (let chunk of encoder) {
        buff.push(chunk);
    }

    const binaryData = Buffer.concat(buff);
    console.log(binaryData);


    const apiKey = '';
    const apiUrl = `http://api.jejuits.go.kr/api/infoParkingStateList`;
    axios.post('http://localhost:8084/v1/avro',
    {
        avro: Buffer.from(binaryData).toString('base64')
    }).then( (res) => {
        console.log(res);
    })
}

main();