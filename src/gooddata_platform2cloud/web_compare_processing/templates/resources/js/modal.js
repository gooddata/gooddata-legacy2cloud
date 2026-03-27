// (C) 2026 GoodData Corporation
// Modal functions
function showModal(rowId, title, description) {
    // Helper function to decode any escaped HTML entities
    function decodeHtml(html) {
        var txt = document.createElement("textarea");
        txt.innerHTML = html;
        return txt.value;
    }

    document.getElementById('modalTitle').textContent = decodeHtml(title);
    document.getElementById('modalDescription').textContent = decodeHtml(description);
    document.getElementById('modalOverlay').classList.add('active');

    // Highlight the selected row
    const row = document.getElementById(rowId);
    const highlightedRow = document.querySelector('.highlight-row');
    if (highlightedRow) {
        highlightedRow.classList.remove('highlight-row');
    }
    row.classList.add('highlight-row');
}

// More robust modal function that extracts data from attributes
function showModalFromData(element) {
    // Get data from the element's data attributes
    const rowId = element.getAttribute('data-row-id');
    const title = element.getAttribute('data-title');
    const description = element.getAttribute('data-description');

    // Helper function to decode any escaped HTML entities
    function decodeHtml(html) {
        var txt = document.createElement("textarea");
        txt.innerHTML = html;
        return txt.value;
    }

    // Log for debugging
    console.log("Modal data:", { rowId, title, description });

    // Set the modal content
    document.getElementById('modalTitle').textContent = decodeHtml(title);

    // For the description, display it as preformatted text if it starts with ERROR:
    const modalDescription = document.getElementById('modalDescription');
    const decodedDesc = decodeHtml(description);

    if (decodedDesc.startsWith('ERROR:')) {
        // Use pre tag for error messages to preserve formatting
        modalDescription.innerHTML = `<pre>${decodedDesc}</pre>`;
    } else {
        modalDescription.textContent = decodedDesc;
    }

    document.getElementById('modalOverlay').classList.add('active');

    // Highlight the selected row
    const row = document.getElementById(rowId);
    const highlightedRow = document.querySelector('.highlight-row');
    if (highlightedRow) {
        highlightedRow.classList.remove('highlight-row');
    }
    row.classList.add('highlight-row');
}

function closeModal() {
    document.getElementById('modalOverlay').classList.remove('active');

    // Remove row highlighting
    const highlightedRow = document.querySelector('.highlight-row');
    if (highlightedRow) {
        highlightedRow.classList.remove('highlight-row');
    }
}

// Close modal when clicking outside of it
document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('modalOverlay').addEventListener('click', function(event) {
        if (event.target === this) {
            closeModal();
        }
    });
});
