// Path: /Main.js

/**
 * Creates the main custom menu when the spreadsheet is opened.
 * This function acts as the main entry point for the user interface.
 */
function onOpen(e) {
  SpreadsheetApp.getUi()
      .createMenu('SuttaWorks Tools')
      .addSubMenu(SpreadsheetApp.getUi().createMenu('Annotation')
          .addItem('Create/Update All (Full)', 'runFullUpdate')
          .addItem('Update New Rows Only (Partial)', 'runPartialUpdate')
          .addItem('Full Update Current Sheet Only', 'runFullUpdateCurrentSheet'))
      // .addSubMenu(SpreadsheetApp.getUi().createMenu('Table of Contents') // Placeholder for the new feature
      //     .addItem('Show TOC Sidebar', 'showTocSidebar'))
      .addToUi();
}