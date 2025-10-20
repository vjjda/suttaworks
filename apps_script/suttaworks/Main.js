// Path: /Main.gs

/**
 * Creates a custom menu when the spreadsheet is opened.
 * @param {object} e The open event object.
 */
function onOpen(e) {
  SpreadsheetApp.getUi()
      .createMenu('Sutta Tools')
      .addItem('Create/Update All (Full)', 'runFullUpdate')
      .addItem('Update New Rows Only (Partial)', 'runPartialUpdate')
      // --- LỰA CHỌN MỚI ---
      .addItem('Full Update Current Sheet Only', 'runFullUpdateCurrentSheet') 
      .addToUi();
}

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
 * [HÀM MỚI] Wrapper function to run a full update on the current sheet only.
 */
function runFullUpdateCurrentSheet() {
  setupAllDropdowns('full_current_sheet');
}

/**
 * [UPGRADED] Creates dropdowns. Added 'full_current_sheet' mode.
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

    // --- LOGIC MỚI: Xử lý 3 chế độ ---
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
    // --- KẾT THÚC LOGIC MỚI ---

    if (sheetsToProcess.length === 0) {
      throw new Error("No sheets to process found matching the criteria.");
    }

    ss.toast(`Starting '${mode}' update for ${sheetsToProcess.length} sheet(s)...`, 'Sutta Tools', 10);

    const fullData = getFullDataObject();
    let totalUpdated = 0;

    for (const sheet of sheetsToProcess) {
      // ... (Phần logic còn lại của hàm giữ nguyên không đổi) ...
      // Nó sẽ lặp qua danh sách sheetsToProcess (giờ chỉ có 1 sheet nếu là partial hoặc full_current_sheet)
      // và áp dụng logic full hoặc partial như cũ.
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
          // Nếu là 'full' hoặc 'full_current_sheet', shouldProcess sẽ luôn là true.
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