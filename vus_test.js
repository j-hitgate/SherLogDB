import http from 'k6/http'
import { check, sleep } from 'k6'

const config = {
    randLogs:      true,
    minLogsToSend: 30,
    maxLogsToSend: 100,
    writersRatio:  0.5,
}

export let options = {
    vus: 20,
    duration: '40s',
}

const chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'

function getRandNum(min, max) {
    return min + Math.floor((max - min) * Math.random())
}

function createStr(maxLen) {
    const len = getRandNum(1, maxLen)
    const str = []
    let j

    for (let i = 0; i < len; i++) {
        j = getRandNum(0, chars.length)
        str.push(chars[j])
    }

    return str.join('')
}

function createStrArray(maxLen, maxStrLen) {
    const len = getRandNum(1, maxLen)
    const arr = []

    for (let i = 0; i < len; i++)
        arr.push(createStr(maxStrLen))

    return arr
}

function createStrObj(maxLen, maxStrLen) {
    const len = getRandNum(1, maxLen)
    const obj = {}

    for (let i = 0; i < len; i++)
        obj[createStr(maxStrLen)] = createStr(maxStrLen)

    return obj
}

function createRandLog() {
    return {
        timestamp: Date.now(),
        level: getRandNum(0, 7),
        traces: createStrArray(20, 50),
        entity: createStr(50),
        entity_id: createStr(50),
        message: createStr(255),
        modules: createStrArray(40, 50),
        labels: createStrArray(20, 50),
        fields: createStrObj(20, 50),
    }
}

function createLog() {
    return {
        timestamp: Date.now(),
        level: 1,
        traces: ['trace1', 'trace2'],
        entity: 'entity',
        entity_id: 'entity id',
        message: 'message',
        modules: ['module1', 'module2', 'module3'],
        labels: ['label1', 'label2', 'label3'],
        fields: {
            'key1': 'val1',
            'key2': 'val2',
        },
    }
}

function createLogs(minLen, maxLen, isRandLogs) {
    const len = getRandNum(minLen, maxLen)
    const getLog = isRandLogs ? createRandLog : createLog
    const logs = []

    for (let i = 0; i < len; i++)
        logs.push(getLog())

    return logs
}

export default function() {
    const userType = Math.random()
    const url = 'http://127.0.0.1:8070'

    const params = {
        headers: {
          'Content-Type': 'application/json',
        },
    }

    if (userType < config.writersRatio) { // writer
        const body = JSON.stringify({
            storage: 'storage',
            logs: createLogs(config.minLogsToSend, config.maxLogsToSend, config.randLogs),
        })
        let res = http.post(url + '/logs', body, params)

        check(res, {
            'status 201': (r) => r.status === 201,
        })
    
    } else { // reader
        const body = JSON.stringify({
            storage: 'storage',
            select: ['timestamp', 'level', 'message', 'fields', 'count[]'],
            limit: 30,
        })

        let res = http.post(url + '/logs/search', body, params)

        check(res, {
            'status 200': (r) => r.status === 200,
        })
    }

    sleep(1)
}