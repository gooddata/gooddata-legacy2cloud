// (C) 2026 GoodData Corporation
// Main script loader
// Order matters - dependencies must be loaded first

// Global variables and core functionality
document.write('<script src="' + resourcesPath + '/js/global.js"></script>');

// Modal dialogs
document.write('<script src="' + resourcesPath + '/js/modal.js"></script>');

// Comparison view
document.write('<script src="' + resourcesPath + '/js/comparison.js"></script>');

// Filtering and sorting
document.write('<script src="' + resourcesPath + '/js/filtering.js"></script>');

// Resizing functionality
document.write('<script src="' + resourcesPath + '/js/resize.js"></script>');

// Sidebar functionality
document.write('<script src="' + resourcesPath + '/js/sidebar.js"></script>');

// Navigation between report types
document.write('<script src="' + resourcesPath + '/js/navigation.js"></script>');
