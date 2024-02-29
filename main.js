const path = require('node:path');
const avro = require('avsc');
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

async function getOpenApiData(url, parameters) {
    try {
        const response = await axios.get(url, { params: parameters });
        return response.data;
    } catch (error) {
        console.error(`Error: ${error}`);
        return null;
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
    let data_time = getDataTime(new Date());
    let logicalTypeDefs = { 'local-timestamp-millis': DateType };
    let schemaPath = path.resolve('./ParkingLotStatus.avsc');
    let schema = avro.parse(schemaPath, { logicalTypes: logicalTypeDefs });

    let encoder = new avro.streams.BlockEncoder(schema);

    const apiKey = '';
    const apiUrl = `http://api.jejuits.go.kr/api/infoParkingStateList`;
    let data = await getOpenApiData(apiUrl, { code: apiKey })
        .catch(error => {
            console.error(`Error: ${error}`);
        });

    data.info.forEach((item) => {
        item.id = Number(item.id);
        item.date_time = data_time;
        encoder.write(item);
    });
    encoder.end();

    const buff = [];
    for await (let chunk of encoder) {
        buff.push(chunk);
    }

    const binaryData = Buffer.concat(buff);
    
    axios.post('http://localhost:8084/v1/avro',
    {
        avro: Buffer.from(binaryData).toString('base64')
    }).then( (res) => {
        console.log(res.data);
    })
}

main();