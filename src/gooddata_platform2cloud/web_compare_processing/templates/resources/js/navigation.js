// (C) 2026 GoodData Corporation
// Report types detection and linking
function detectReportTypes() {
    // List of all possible report types in desired order
    const reportTypes = ['insight', 'dashboard', 'report'];

    // Get current path
    const currentPath = window.location.pathname;
    const currentFile = currentPath.split('/').pop();

    // Simple mapping for HTML files to report types
    const fileTypeMap = {
        'insights_web_compare.html': 'insight',
        'dashboards_web_compare.html': 'dashboard',
        'reports_web_compare.html': 'report'
    };

    // Try to determine current type based on filename
    let currentType = '';
    let prefix = '';

    // Use the server-provided isPrefixed instead of trying to determine from URL
    // isPrefixed is set in the template as a global variable

    // Extract prefix and type from filename
    const filePattern = /^(.*?)?(insights|dashboards|reports)_web_compare\.html$/i;
    const match = currentFile.match(filePattern);

    if (match) {
        const typeMap = {
            'insights': 'insight',
            'dashboards': 'dashboard',
            'reports': 'report'
        };

        currentType = typeMap[match[2].toLowerCase()];
        prefix = match[1] || '';
    } else {
        // Default to report if we can't determine
        currentType = 'report';
    }

    // Ensure prefix doesn't end with underscore to avoid duplicates
    const cleanPrefix = prefix.endsWith('_') ? prefix.slice(0, -1) : prefix;

    // Determine path to the directory containing the current file
    let dirPath = '';
    if (isPrefixed) {
        // If we're in a prefixed directory, just use the current directory
        dirPath = './';
    } else {
        // If we're in the main directory, use the same directory
        dirPath = './';
    }

    // Prepare results array with initial objects for each type
    const results = reportTypes.map(type => ({
        type,
        present: type === currentType, // We know current type is present
        checked: type === currentType  // Mark current type as checked
    }));

    // Count how many types we need to check
    let toCheck = reportTypes.filter(type => type !== currentType).length;

    // Check if all types have been processed
    function checkComplete() {
        if (toCheck === 0) {
            updateNavigationLinks(results, currentType, cleanPrefix, dirPath, isPrefixed);
        }
    }

    // Immediately check if we only have one type
    if (toCheck === 0) {
        checkComplete();
        return;
    }

    // Get proper indicators path
    let indicatorsPath;
    if (isPrefixed) {
        // If we're in a prefixed directory, the indicators are in the same directory
        indicatorsPath = 'indicators';
    } else {
        // If we're in the main directory, indicators are also in the same directory
        indicatorsPath = 'indicators';
    }

    // For each report type (except current one), try to load its indicator script
    reportTypes.forEach(type => {
        if (type === currentType) return; // Skip current type, we already know it's present

        // Check if the corresponding HTML file exists by loading a tiny script
        const scriptEl = document.createElement('script');
        const timestamp = new Date().getTime(); // Prevent caching

        // The server generates indicator files with clean_prefix (without trailing underscore)
        // So we need to use cleanPrefix to match the server's format
        scriptEl.src = `${dirPath}${indicatorsPath}/${cleanPrefix}${type}.js?t=${timestamp}`;

        console.log(`Checking for indicator: ${scriptEl.src}`);

        scriptEl.async = true;

        // Set timeout to handle case where script doesn't load or execute
        const timeoutId = setTimeout(() => {
            const resultItem = results.find(r => r.type === type);
            if (resultItem && !resultItem.checked) {
                resultItem.present = false;
                resultItem.checked = true;
                toCheck--;
                checkComplete();
            }
        }, 2000);

        // Script loaded successfully
        scriptEl.onload = function() {
            clearTimeout(timeoutId);

            // Check if the presence function exists
            const funcName = `${type}_present`;
            if (typeof window[funcName] === 'function') {
                const resultItem = results.find(r => r.type === type);
                if (resultItem && !resultItem.checked) {
                    resultItem.present = true;
                    resultItem.checked = true;
                    toCheck--;
                    checkComplete();
                }
            } else {
                const resultItem = results.find(r => r.type === type);
                if (resultItem && !resultItem.checked) {
                    resultItem.present = false;
                    resultItem.checked = true;
                    toCheck--;
                    checkComplete();
                }
            }
        };

        // Script failed to load
        scriptEl.onerror = function() {
            clearTimeout(timeoutId);
            const resultItem = results.find(r => r.type === type);
            if (resultItem && !resultItem.checked) {
                resultItem.present = false;
                resultItem.checked = true;
                toCheck--;
                checkComplete();
            }
        };

        // Add the script to the document
        document.head.appendChild(scriptEl);
    });
}

// Update navigation links in the sidebar
function updateNavigationLinks(results, currentType, prefix, dirPath, isPrefixed) {
    const linksContainer = document.getElementById('report-links');
    if (!linksContainer) {
        console.warn('Links container not found');
        return;
    }

    linksContainer.innerHTML = ''; // Clear existing links

    // Map for display names (capitalized and pluralized)
    const displayNames = {
        'insight': 'Insights',
        'dashboard': 'Dashboards',
        'report': 'Reports'
    };

    // Sort the results to ensure consistent order: dashboard, insight, report
    const sortOrder = {'dashboard': 1, 'insight': 2, 'report': 3};
    results.sort((a, b) => (sortOrder[a.type] || 99) - (sortOrder[b.type] || 99));

    // Ensure prefix doesn't end with underscore to avoid duplicates
    const cleanPrefix = prefix.endsWith('_') ? prefix.slice(0, -1) : prefix;

    results.forEach(result => {
        const { type, present } = result;
        const linkItem = document.createElement('div');
        const displayName = displayNames[type] || type.charAt(0).toUpperCase() + type.slice(1) + 's';

        if (type === currentType) {
            // For current type, no link is needed
            linkItem.className = 'menu-item active';
            linkItem.textContent = displayName;
        } else if (present) {
            // For available types, create a link that fills the entire menu item
            linkItem.className = 'menu-item';
            const link = document.createElement('a');

            if (isPrefixed) {
                // If we're in a prefix directory, link to the same prefix directory for other types
                link.href = `${cleanPrefix}_${type}s_web_compare.html`;
            } else {
                // We're in the main output directory
                link.href = `${type}s_web_compare.html`;
            }

            link.textContent = displayName;
            linkItem.appendChild(link);
        } else {
            // For disabled types, no link is needed
            linkItem.className = 'menu-item disabled';
            linkItem.textContent = displayName;
        }

        linksContainer.appendChild(linkItem);
    });

    // Log the links for debugging
    console.log('Updated navigation links:', linksContainer.innerHTML);
}

// Check if parent directory has indicator for the current type
function checkParentIndicator(currentType, callback) {
    if (!currentType) {
        callback(false);
        return;
    }

    // Create script element to load the indicator from parent directory
    const scriptEl = document.createElement('script');
    const timestamp = new Date().getTime(); // Prevent caching

    // Path to parent directory indicator
    scriptEl.src = `../indicators/${currentType}.js?t=${timestamp}`;

    console.log(`Checking for parent indicator at: ${scriptEl.src}`);

    scriptEl.async = true;

    // Set timeout to handle case where script doesn't load or execute
    const timeoutId = setTimeout(() => {
        console.log('Parent indicator not found (timeout)');
        callback(false);
    }, 2000);

    // Script loaded successfully
    scriptEl.onload = function() {
        clearTimeout(timeoutId);

        // Check if the presence function exists
        const funcName = `${currentType}_present`;
        if (typeof window[funcName] === 'function') {
            console.log('Parent indicator found');
            callback(true);
        } else {
            console.log('Parent indicator loaded but function not found');
            callback(false);
        }
    };

    // Script failed to load
    scriptEl.onerror = function() {
        clearTimeout(timeoutId);
        console.log('Parent indicator not found (error)');
        callback(false);
    };

    // Add the script to the document
    document.head.appendChild(scriptEl);
}

// Set up sidebar navigation based on page location
function setupSidebarNavigation() {
    // Get the current path and filename
    const currentPath = window.location.pathname;
    const currentFile = currentPath.split('/').pop();

    // Use the server-provided isPrefixed variable instead of detecting from URL
    // isPrefixed is set in the template as a global variable

    // Try to extract prefix and type from the filename
    const filePattern = /^(.*?)?(insights|dashboards|reports)_web_compare\.html$/i;
    const match = currentFile.match(filePattern);

    let currentType = '';
    let prefix = '';

    if (match) {
        // Type mapping
        const typeMap = {
            'insights': 'insight',
            'dashboards': 'dashboard',
            'reports': 'report'
        };

        currentType = typeMap[match[2].toLowerCase()];
        prefix = match[1] || '';
    }

    // Ensure prefix doesn't end with underscore to avoid duplicates
    const cleanPrefix = prefix.endsWith('_') ? prefix.slice(0, -1) : prefix;

    // Set up navigation between object types (report, dashboard, insight)
    detectReportTypes();

    // If we're in a prefixed directory, check for parent indicator before showing "Back to Master" link
    if (isPrefixed && cleanPrefix) {
        const prefixNavElement = document.querySelector('.prefix-navigation');
        if (prefixNavElement) {
            // Initially hide the navigation element
            prefixNavElement.style.display = 'none';

            // Check if parent has indicator for this type
            console.log(`Checking parent indicator for type: ${currentType}`);
            checkParentIndicator(currentType, function(hasParentIndicator) {
                if (hasParentIndicator) {
                    console.log(`Found parent indicator for ${currentType}, showing back to parent link`);
                    // Show the navigation element if parent indicator exists
                    prefixNavElement.style.display = '';

                    // Set the correct link
                    const linkElement = prefixNavElement.querySelector('a');
                    if (linkElement) {
                        linkElement.href = `../${currentType}s_web_compare.html`;
                    }
                } else {
                    console.log(`No parent indicator found for ${currentType}, hiding back to parent link`);
                }
            });
        }
    }

    // If we're in the main directory, make sure prefix links are working
    if (!isPrefixed) {
        const prefixItems = document.querySelectorAll('.prefix-item a');
        prefixItems.forEach(link => {
            const prefixName = link.querySelector('.prefix-name').textContent.trim();
            link.href = `${prefixName}/${prefixName}_${currentType}s_web_compare.html`;
            // Add title attribute for better UX
            link.title = `Switch to client '${prefixName}'`;
        });
    }
}
