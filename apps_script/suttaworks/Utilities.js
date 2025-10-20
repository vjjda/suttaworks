// Path: /Utilities.gs

/**
 * Finds ALL sheets that match a regex pattern.
 * @param {RegExp} pattern The regex pattern to test against sheet names.
 * @returns {Array<GoogleAppsScript.Spreadsheet.Sheet>} An ARRAY of matching sheet objects.
 */
function findAllSheetsByPattern(pattern) {
  const allSheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
  const matchingSheets = [];
  for (const sheet of allSheets) {
    if (pattern.test(sheet.getName())) {
      matchingSheets.push(sheet);
    }
  }
  return matchingSheets;
}

/**
 * Finds ALL column indices that match a regex pattern.
 * @param {Array<string>} headers An array of header names.
 * @param {RegExp} pattern The regex pattern to test against header names.
 * @returns {Array<number>} An ARRAY of matching column indices.
 */
function findAllColumnIndicesByPattern(headers, pattern) {
  const indices = [];
  for (let i = 0; i < headers.length; i++) {
    if (pattern.test(headers[i])) {
      indices.push(i);
    }
  }
  return indices;
}

/**
 * Finds the FIRST column index that matches a regex pattern.
 * @param {Array<string>} headers An array of header names.
 * @param {RegExp} pattern The regex pattern to test against header names.
 * @returns {number} The index of the first matching column, or -1 if not found.
 */
function findColumnIndexByPattern(headers, pattern) {
  for (let i = 0; i < headers.length; i++) {
    if (pattern.test(headers[i])) {
      return i;
    }
  }
  return -1;
}