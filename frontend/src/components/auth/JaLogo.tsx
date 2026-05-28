// J.A. Uniforms stacked logo
// Bold sans "J" + crossbar-less triangular "A", with the word "uniforms"
// set lighter below. The whole mark inherits the parent's `color`, so you
// can paint it brass on the dark panel and ink on the cream panel.

interface JaLogoProps {
  size?: 'lg' | 'md' | 'xs'
  className?: string
}

export default function JaLogo({ size = 'lg', className = '' }: JaLogoProps) {
  return (
    <div className={`ja-logo ja-logo--${size} ${className}`} aria-label="J.A. Uniforms">
      <svg viewBox="0 0 88 60" className="ja-logo-mark" fill="currentColor">
        {/* J — horizontal top bar + straight vertical stem + curled hook */}
        <path d="M 4 4 L 36 4 L 36 38 C 36 50, 28 56, 20 56 C 8 56, 0 50, 0 40 L 10 40 C 10 47, 14 52, 20 52 C 26 52, 28 47, 28 38 L 28 14 L 4 14 Z" />
        {/* A — pure triangle, no crossbar, inverted-V hollow */}
        <path d="M 40 56 L 60 4 L 80 56 L 70 56 L 60 26 L 50 56 Z" fillRule="evenodd" />
      </svg>
      <div className="ja-logo-word">uniforms</div>
    </div>
  )
}
