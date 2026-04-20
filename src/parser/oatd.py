"""
OATD HTML parser — extracts structured thesis metadata from search result pages.

The extraction is done entirely in-browser via JavaScript (opencli browser eval),
returning clean JSON that Python simply saves. This avoids transferring large HTML
over the CLI bridge.
"""

# JavaScript code to extract all 30 results from a search page.
# Runs inside Chrome via opencli browser eval.
EXTRACT_SEARCH_RESULTS_JS = r"""
(function() {
  var results = [];
  var divs = document.querySelectorAll('div.result');
  
  divs.forEach(function(div) {
    try {
      // --- Title ---
      var citeEl = div.querySelector('cite.etdTitle span');
      var title = citeEl ? citeEl.textContent.trim() : '';
      
      // --- Author ---
      // First <span> directly inside the first <p> child (not inside cite)
      var firstP = div.querySelector('p');
      var authorSpan = firstP ? firstP.querySelector(':scope > span') : null;
      var author = authorSpan ? authorSpan.textContent.trim() : '';
      
      // --- Degree info (contains year + university) ---
      var degreeEl = div.querySelector('p.degree');
      var degreeText = degreeEl ? degreeEl.textContent.trim() : '';
      
      // Extract year from degree text: "Degree: PhD, CS, 2023, University"
      var yearMatch = degreeText.match(/\b(19|20)\d{2}\b/);
      var year = yearMatch ? yearMatch[0] : '';
      
      // --- University ---
      var pubEl = div.querySelector('p.degree span[itemprop="publisher"]');
      var university = pubEl ? pubEl.textContent.trim() : '';
      
      // --- External URL (PDF/thesis link) ---
      var linkEl = div.querySelector('p.links a[href]');
      var url = linkEl ? linkEl.href : '';
      
      // --- Abstract ---
      var absEl = div.querySelector('div.abstract');
      var abstract = absEl ? absEl.textContent.trim() : '';
      
      // --- Keywords ---
      var kwEl = div.querySelector('p.keywords');
      var keywords = '';
      if (kwEl) {
        keywords = kwEl.textContent.replace('Subjects/Keywords:', '').trim();
      }
      
      // --- Detail page URL ---
      var detailEl = div.querySelector('a[href*="record?record="]');
      var detailUrl = detailEl ? detailEl.href : '';
      
      // --- Record ID from HTML comment above the div ---
      // Comments like: <!-- Repository: X | ID: Y | URL -->
      var prevSibling = div.previousSibling;
      var recordId = '';
      while (prevSibling) {
        if (prevSibling.nodeType === 8) { // Comment node
          var commentText = prevSibling.textContent;
          var idMatch = commentText.match(/ID:\s*([^\|]+)/);
          if (idMatch) {
            recordId = idMatch[1].trim();
          }
          break;
        }
        prevSibling = prevSibling.previousSibling;
      }
      
      if (title || url) {
        results.push({
          title: title,
          author: author,
          university: university,
          year: year,
          degree: degreeText.replace(/^Degree:\s*/, ''),
          url: url,
          abstract: abstract,
          keywords: keywords,
          detail_url: detailUrl,
          record_id: recordId
        });
      }
    } catch(e) {
      // Skip malformed entries
    }
  });
  
  return JSON.stringify(results);
})()
"""

# JavaScript to extract result count and pagination info
EXTRACT_PAGINATION_JS = r"""
(function() {
  var text = document.body.innerText;
  var match = text.match(/Showing records\s+(\d+)\s*[–-]\s*(\d+)\s+of\s+([\d,]+)\s+total/);
  var hasError = document.querySelector('h3') && document.querySelector('h3').textContent.includes('Oops');
  
  if (match) {
    return JSON.stringify({
      start: parseInt(match[1]),
      end: parseInt(match[2]),
      total: parseInt(match[3].replace(/,/g, '')),
      has_error: false
    });
  }
  
  return JSON.stringify({
    start: 0,
    end: 0,
    total: 0,
    has_error: hasError || false
  });
})()
"""
