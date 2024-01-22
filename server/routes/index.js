const express = require('express')
const app = express()

app.use(require('./ExcelGatillos'))
app.use(require('./ReporteMasterBd'))

module.exports=app;