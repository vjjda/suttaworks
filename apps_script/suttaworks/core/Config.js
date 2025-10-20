// Path: /Config.gs

/**
 * Reads all configurations from the "Config" sheet and converts them to RegExp objects.
 * @returns {object} A configuration object with ready-to-use RegExp patterns.
 */
function getConfigsFromSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configSheet = ss.getSheetByName('Config');
  if (!configSheet) {
    throw new Error('Sheet "Config" not found. Please create and set it up.');
  }
  const data = configSheet.getRange('A2:B').getValues();
  const configs = {};
  for (const row of data) {
    const key = row[0];
    let value = row[1];
    if (key && value) {
      const match = value.match(/^\/(.*)\/([gimuy]*)$/);
      if (match) {
        configs[key] = new RegExp(match[1], match[2]);
      } else {
        configs[key] = new RegExp(value, 'i');
      }
    }
  }
  return {
    sheetPattern: configs['Sheet Name Pattern'],
    topicColumnPattern: configs['Topic Column Pattern'],
    uidColumnPattern: configs['UID Column Pattern']
  };
}