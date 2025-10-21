// Path: apps_script/suttaworks/features/annotation/index.js

// --- Wrapper Functions (from old Main.js) ---

/**
 * Wrapper function to run the full update on all matching sheets.
 */
function runFullUpdate() {
  setupAllDropdowns('full');
}

/**
 * Wrapper function to run the partial update on the active sheet.
 */
function runPartialUpdate() {
  setupAllDropdowns('partial');
}

/**
 * Wrapper function to run a full update on the current sheet only.
 */
function runFullUpdateCurrentSheet() {
  setupAllDropdowns('full_current_sheet');
}


// --- Main Logic (from old Main.js) ---

/**
 * Creates dropdowns based on the selected mode.
 * @param {string} mode - The update mode: 'full', 'partial', or 'full_current_sheet'.
 */
function setupAllDropdowns(mode = 'full') {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  try {
    const configs = getConfigsFromSheet();
    if (!configs.sheetPattern || !configs.topicColumnPattern || !configs.uidColumnPattern) {
        throw new Error('Configuration in "Config" sheet is missing or incomplete.');
    }
    
    let sheetsToProcess = [];
    const activeSheet = ss.getActiveSheet();

    switch (mode) {
      case 'partial':
      case 'full_current_sheet':
        if (configs.sheetPattern.test(activeSheet.getName())) {
          sheetsToProcess.push(activeSheet);
        } else {
          ss.toast('Current sheet does not match the configured pattern. Operation stopped.', 'Sutta Tools', 10);
          return;
        }
        break;
      case 'full':
      default:
        sheetsToProcess = findAllSheetsByPattern(configs.sheetPattern);
        break;
    }

    if (sheetsToProcess.length === 0) {
      throw new Error("No sheets to process found matching the criteria.");
    }

    ss.toast(`Starting '${mode}' update for ${sheetsToProcess.length} sheet(s)...`, 'Sutta Tools', 10);

    const fullData = getFullDataObject();
    let totalUpdated = 0;

    for (const sheet of sheetsToProcess) {
      const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      const topicColumnIndices = findAllColumnIndicesByPattern(headers, configs.topicColumnPattern);
      const suttaUidColIndex = findColumnIndexByPattern(headers, configs.uidColumnPattern);

      if (topicColumnIndices.length === 0 || suttaUidColIndex === -1) continue;
      
      const numRows = sheet.getMaxRows() - 1;
      if (numRows <= 0) continue;
      
      const uidValues = sheet.getRange(2, suttaUidColIndex + 1, numRows, 1).getValues();

      for (const topicColIndex of topicColumnIndices) {
        const topicColumnRange = sheet.getRange(2, topicColIndex + 1, numRows, 1);
        const existingRules = topicColumnRange.getDataValidations();
        const newRules = [];

        for (let i = 0; i < numRows; i++) {
          const existingRule = existingRules[i][0];
          const shouldProcess = (mode === 'partial') ? (existingRule == null) : true;
          
          if (shouldProcess) {
            let suttaUid = uidValues[i][0];
            if (suttaUid && typeof suttaUid.toString === 'function') {
              suttaUid = suttaUid.toString().trim();
            }
            
            let newRule = null;
            if (suttaUid && fullData[suttaUid]) {
              const topicsForSutta = fullData[suttaUid];
              let choicesList = [];
              Object.keys(topicsForSutta).forEach(topic => {
                Object.keys(topicsForSutta[topic]).forEach(subTopic => {
                  choicesList.push(`${topic} / ${subTopic}`);
                });
              });
              choicesList.sort();

              if (choicesList.length > 0) {
                newRule = SpreadsheetApp.newDataValidation()
                    .requireValueInList(choicesList)
                    .setAllowInvalid(false)
                    .build();
              }
            }
            newRules.push([newRule]);
            if (newRule) totalUpdated++;
          } else {
            newRules.push([existingRule]);
          }
        }
        topicColumnRange.setDataValidations(newRules);
      }
    }
    
    ss.toast(`Finished! ${totalUpdated} dropdowns were created/updated.`, 'Sutta Tools', 10);

  } catch (err) {
    Logger.log("Error in setupAllDropdowns: " + err.message);
    ui.alert("An error occurred: " + err.message);
  }
}


// --- Data Functions (from old Data.js) ---

/**
 * Gets the full JSON data object by parsing the string from the cache or source.
 * @returns {object} The parsed JSON object.
 */
function getFullDataObject() {
    const jsonString = getSuttaJsonString();
    return JSON.parse(jsonString);
}

/**
 * Gets the raw JSON string, using a chunking cache strategy for performance.
 * @returns {string} The raw JSON string.
 */
function getSuttaJsonString() {
    const cache = CacheService.getScriptCache();
    const CHUNK_CACHE_KEY = 'SUTTA_JSON_CHUNK_';
    const CHUNK_COUNT_KEY = 'SUTTA_JSON_CHUNK_COUNT';
    const chunkCount = cache.get(CHUNK_COUNT_KEY);

    if (chunkCount != null) {
        let jsonDataString = '';
        const keys = [];
        for (let i = 0; i < chunkCount; i++) { keys.push(CHUNK_CACHE_KEY + i); }
        const cachedChunks = cache.getAll(keys);
        for (let i = 0; i < chunkCount; i++) {
            if (cachedChunks[keys[i]]) { jsonDataString += cachedChunks[keys[i]]; } 
            else { jsonDataString = ''; break; }
        }
        if (jsonDataString.length > 0) { return jsonDataString; }
    }

    Logger.log('Cache MISS. Fetching data from public GitHub URL.');
    
    const JSON_DATA_URL = "https://raw.githubusercontent.com/vjjda/suttaworks/main/data/public/cips/cips_sutta.json";
    
    const options = {
      'muteHttpExceptions': true
    };
    
    const response = UrlFetchApp.fetch(JSON_DATA_URL, options);
    
    if (response.getResponseCode() !== 200) {
        throw new Error(`Failed to fetch data. Response code: ${response.getResponseCode()}`);
    }

    const content = response.getContentText();
    const CHUNK_SIZE = 90000;
    const numChunks = Math.ceil(content.length / CHUNK_SIZE);
    const chunks = {};
    for (let i = 0; i < numChunks; i++) {
        chunks[CHUNK_CACHE_KEY + i] = content.substring(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    }
    chunks[CHUNK_COUNT_KEY] = numChunks.toString();
    cache.putAll(chunks, 21600);
    return content;
}