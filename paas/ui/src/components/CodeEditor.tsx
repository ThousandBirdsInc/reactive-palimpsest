// Overlay code editor: a transparent <textarea> stacked over a highlighted
// <pre>. The textarea drives all interaction and sizing; the <pre> renders the
// syntax-highlighted text behind it. Both layers reuse the `.sql-editor` metric
// class so their glyphs line up exactly, and the textarea's scroll position is
// mirrored onto the <pre>.

import {
  forwardRef,
  ReactNode,
  TextareaHTMLAttributes,
  UIEvent,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";

interface Props extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  value: string;
  highlight: (src: string) => string;
  /** Extra absolutely-positioned content (e.g. a completion menu). */
  overlay?: ReactNode;
}

export const CodeEditor = forwardRef<HTMLTextAreaElement, Props>(function CodeEditor(
  { value, highlight, overlay, className, onScroll, ...rest },
  ref,
) {
  const preRef = useRef<HTMLPreElement>(null);
  const innerRef = useRef<HTMLTextAreaElement>(null);
  useImperativeHandle(ref, () => innerRef.current as HTMLTextAreaElement, []);

  function mirrorScroll() {
    const pre = preRef.current;
    const textarea = innerRef.current;
    if (pre && textarea) {
      pre.scrollTop = textarea.scrollTop;
      pre.scrollLeft = textarea.scrollLeft;
    }
  }

  // Editing or accepting a completion can scroll the textarea to the caret
  // without firing onScroll, so re-mirror whenever the value changes.
  useEffect(mirrorScroll, [value]);

  function syncScroll(event: UIEvent<HTMLTextAreaElement>) {
    mirrorScroll();
    onScroll?.(event);
  }

  return (
    <div className="code-editor-wrap">
      <pre ref={preRef} className="sql-editor code-pre" aria-hidden="true">
        {/* Trailing newline keeps the final line's height in sync with the
            textarea when the value ends in a newline. */}
        <code dangerouslySetInnerHTML={{ __html: `${highlight(value)}\n` }} />
      </pre>
      <textarea
        {...rest}
        ref={innerRef}
        value={value}
        spellCheck={false}
        className={className ? `sql-editor code-input ${className}` : "sql-editor code-input"}
        onScroll={syncScroll}
      />
      {overlay}
    </div>
  );
});
