export enum FeatureFlag {
  flagDebugConsoleLogging = 'flagDebugConsoleLogging',
  flagDisableWebsockets = 'flagDisableWebsockets',
  flagSidebarResources = 'flagSidebarResources',
  flagDisableAutoLoadDefaults = 'flagDisableAutoLoadDefaults',
  flagLegacyRunsPage = 'flagLegacyRunsPage',
  flagAssetSelectionSyntax = 'flagAssetSelectionSyntax',
  flagRunSelectionSyntax = 'flagRunSelectionSyntax',
  flagAssetSelectionWorker = 'flagAssetSelectionWorker',
  flagOpSelectionSyntax = 'flagOpSelectionSyntax',

  // Flags for tests
  __TestFlagDefaultNone = '__TestFlagDefaultNone',
  __TestFlagDefaultTrue = '__TestFlagDefaultTrue',
  __TestFlagDefaultFalse = '__TestFlagDefaultFalse',
}
