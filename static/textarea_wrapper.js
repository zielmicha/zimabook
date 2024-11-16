class TextareaWrapper extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this.shadowRoot.innerHTML = `
      <style>
        textarea {
          width: 100%;
          min-height: 100px;
          padding: 8px;
          box-sizing: border-box;
        }
        textarea[data-edited=true] {
          background: #eee;
        }
      </style>
      <textarea></textarea>
    `;
    
    this._textarea = this.shadowRoot.querySelector('textarea');
    this._originalContent = '';
    this._setEdited(false);
    
    this._textarea.addEventListener('input', () => {
        this._setEdited(true);
	this._checkNoLongerEdited();
	
	let modifyEvent = this.getAttribute('modify-event')
	if (modifyEvent) {
	    let parsedEvent = JSON.parse(modifyEvent)
	    let params = parsedEvent.params
	    params.content = this.content
	    socket.emit(parsedEvent.name, params)
	}
    });

    this._textarea.addEventListener('focus', () => {
        let focusEvent = this.getAttribute('focus-event')
	if (focusEvent) {
	    let parsedEvent = JSON.parse(focusEvent)
	    socket.emit(parsedEvent.name, parsedEvent.params)
	}
    });
  }

  _setEdited(edited) {
      this._edited = edited;
      this._textarea.dataset.edited = edited
  }

  _checkNoLongerEdited() {
      if (this._textarea.value == this._originalContent) {
	  console.log('no longer considered edited')
	  this._setEdited(false)
      }
  }
    
  static get observedAttributes() {
      return ['text', 'focus'];
  }
  
  attributeChangedCallback(name, oldValue, newValue) {
      if (name === 'text') {
	  this._originalContent = newValue;
	  if (!this._edited) {
	      this._textarea.value = newValue;
	  }
	  this._checkNoLongerEdited()
      }
      if (name == 'focus') {
	  if (newValue)
	      this._textarea.focus({preventScroll: true})
	  else
	      this._textarea.blur()
      }
  }
  
  connectedCallback() {
    this._originalContent = this.getAttribute('text') || '';
    this._textarea.value = this._originalContent;
  }
  
  get content() {
    return this._textarea.value;
  }
}

customElements.define('textarea-wrapper', TextareaWrapper);
