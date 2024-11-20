require('dotenv').config();
const { google } = require('googleapis');
const path = require('path');
const cliProgress = require('cli-progress');

async function registration(url, user) {
    try {
        let response = await fetch(url + 'auth/registration', {
            method: "POST",
            headers: {
                "Content-Type": "Application/json",
            },
            body: JSON.stringify({ username: user })
        })
        const data = await response.json();
        if (data.statusCode === 400) {
            console.log('Пользователь уже существует');
        }
    } catch (error) {
        console.error('Ошибка регистрации:', error);
    }
}

async function login(url, user) {
    try {
        let response = await fetch(url + 'auth/login', {
            method: "POST",
            headers: {
                "Content-Type": "Application/json",
            },
            body: JSON.stringify({ username: user })
        });
        const data = await response.json();
        return data.token;
    } catch (error) {
        console.log('Ошибка логина: ', error);
    }
}

async function clientsList(limit, offset, url, token) {
    try {
        let response = await fetch(url + `clients?limit=${limit}&offset=${offset}`, {
            method: "GET",
            headers: {
                "Content-Type": "Application/json",
                Authorization: token
            }
        })
        return await response.json();
    } catch (error) {
        console.error('Ошибка получения списка клиентов:', error);
    }
}

async function clientsStatus(userIds, url, token) {
    try {
        let response = await fetch(url + 'clients', {
            method: "POST",
            headers: {
                "Content-Type": "Application/json",
                Authorization: token
            },
            body: JSON.stringify({ userIds })
        })
        return await response.json();
    } catch (error) {
        console.error('Ошибка получения статусов клиентов:', error);
    }
}

async function writeToGoogleSheet(dataToWrite) {
    const auth = new google.auth.GoogleAuth({
        keyFile: path.join(__dirname, 'credentials.json'),
        scopes: 'https://www.googleapis.com/auth/spreadsheets',
    });
    const client = await auth.getClient();
    const spreadsheetId = process.env.SPREADSHEET_ID;
    const googleSheets = google.sheets({ version: 'v4', auth: client });
    const sheetName = 'Лист1';
    const header = ['ID', 'First Name', 'Last Name', 'Gender', 'Address', 'City', 'Phone', 'Email', 'Status'];

    const progressBar = new cliProgress.SingleBar({
        format: 'Прогресс [{bar}] {percentage}% | {value}/{total} записей',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true,
    });

    const sheetInfo = await googleSheets.spreadsheets.get({
        spreadsheetId,
        includeGridData: false,
    });

    const sheet = sheetInfo.data.sheets.find(s => s.properties.title === sheetName);
    const currentRows = sheet.properties.gridProperties.rowCount;
    const rowsNeeded = dataToWrite.length + 1;

    if (rowsNeeded > currentRows) {
        const rowsToAdd = rowsNeeded - currentRows;
        await googleSheets.spreadsheets.batchUpdate({
            spreadsheetId,
            requestBody: {
                requests: [
                    {
                        appendDimension: {
                            sheetId: sheet.properties.sheetId,
                            dimension: 'ROWS',
                            length: rowsToAdd,
                        },
                    },
                ],
            },
        });
    }

    const getRows = await googleSheets.spreadsheets.values.get({
        spreadsheetId,
        range: sheetName,
    });
    const rows = getRows.data.values || [];
    let startRow = rows.length + 1;

    if (rows.length === 0) {
        await googleSheets.spreadsheets.values.update({
            spreadsheetId,
            range: `${sheetName}!A1`,
            valueInputOption: 'RAW',
            requestBody: {
                values: [header],
            },
        });
        startRow = 2;
    }

    const BATCH_SIZE = 1000;
    const RETRY_DELAY = 5000;

    progressBar.start(dataToWrite.length, 0);
    for (let i = 0; i < dataToWrite.length; i += BATCH_SIZE) {
        const batchData = dataToWrite.slice(i, i + BATCH_SIZE).map(client => [
            client.id,
            client.firstName,
            client.lastName,
            client.gender,
            client.address,
            client.city,
            client.phone,
            client.email,
            client.status,
        ]);
        let attempt = 0;
        let success = false;
        while (!success && attempt < 5) {
            try {
                await googleSheets.spreadsheets.values.update({
                    spreadsheetId,
                    range: `${sheetName}!A${startRow}`,
                    valueInputOption: 'RAW',
                    requestBody: {
                        values: batchData,
                    },
                });
                success = true;
            } catch (error) {
                if (error.code === 429) {
                    console.warn('Превышен лимит запросов. Ждем перед повторной попыткой...');
                    await delay(RETRY_DELAY);
                    attempt++;
                } else {
                    console.error('Ошибка при записи данных:', error);
                    throw error;
                }
            }
        }
        if (!success) {
            throw new Error(`Не удалось записать данные для строк ${startRow}–${startRow + BATCH_SIZE}`);
        }
        startRow += BATCH_SIZE;
        progressBar.update(Math.min(i + BATCH_SIZE, dataToWrite.length));
    }
    progressBar.stop();
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    try {
        let dataToWrite = []
        const URL = process.env.URL_API
        const USER = process.env.USER_NAME

        await registration(URL, USER)
        const token = await login(URL, USER)

        let offset = 0;
        const limit = 1000;

        while (true) {
            const clients = await clientsList(limit, offset, URL, token);
            if (!clients || clients.length === 0) {
                break;
            }
            offset += limit;
            const userIds = clients.map(item => item.id);
            const statuses = await clientsStatus(userIds, URL, token);
            const statusMap = new Map(statuses.map(status => [status.id, status.status]))

            clients.forEach(client => {
                dataToWrite.push({
                    id: client.id,
                    firstName: client.firstName,
                    lastName: client.lastName,
                    gender: client.gender,
                    address: client.address,
                    city: client.city,
                    phone: client.phone,
                    email: client.email,
                    status: statusMap.get(client.id) || 'Unknown',
                });
            });
        }
        writeToGoogleSheet(dataToWrite);
    } catch (error) {
        console.log('Ошибка: ', error);
    }
}

main()