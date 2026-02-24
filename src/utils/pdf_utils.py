from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import inch
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from pathlib import Path
import os
import re
from io import BytesIO
from typing import Optional


from reportlab.lib.utils import ImageReader


def create_pdf(
    markdown_text: str,
    chart_image: Optional[BytesIO] = None,
    chart_images: Optional[list] = None,
) -> bytes:
    """Convert Markdown text to PDF using ReportLab with enhanced styling

    Args:
        markdown_text: Markdown content
        chart_image: (Deprecated) Single chart image for backward compatibility
        chart_images: List of BytesIO objects containing chart images (PNG/JPEG)

    Features:
        - Bold text support (**text**)
        - Heading hierarchy with proper sizing
        - Multiple chart images support
        - Reduced chart sizes for better readability
    """
    # Backward compatibility: merge single chart_image into list
    all_charts = []
    if chart_images:
        all_charts.extend(chart_images)
    elif chart_image:
        all_charts.append(chart_image)
    # Get project root and fonts directory
    project_root = Path(__file__).parent.parent.parent
    fonts_dir = project_root / "fonts"

    # Try to find Korean fonts (regular and bold)
    font_paths = [
        # 1. 프로젝트 내 폰트 (우선)
        fonts_dir / "NanumGothic.ttf",
        fonts_dir / "MALGUN.TTF",
        fonts_dir / "malgun.ttf",
        # 2. macOS - 사용자 폰트 (Homebrew 설치 위치)
        Path.home() / "Library" / "Fonts" / "NanumGothic.ttf",
        Path.home() / "Library" / "Fonts" / "NanumBarunGothic.ttf",
        # 3. macOS - 시스템 폰트
        Path("/System/Library/Fonts/Supplemental/AppleGothic.ttf"),
        Path("/Library/Fonts/AppleGothic.ttf"),
        # 4. Windows
        Path("C:/Windows/Fonts/malgun.ttf"),
        Path("C:/Windows/Fonts/MALGUN.TTF"),
    ]

    bold_font_paths = [
        # 1. 프로젝트 내 폰트
        fonts_dir / "NanumGothicBold.ttf",
        fonts_dir / "MALGUNBD.TTF",
        fonts_dir / "malgunbd.ttf",
        # 2. macOS - 사용자 폰트
        Path.home() / "Library" / "Fonts" / "NanumGothicBold.ttf",
        Path.home() / "Library" / "Fonts" / "NanumBarunGothicBold.ttf",
        # 3. macOS - 시스템 폰트 (Bold 없으면 Regular 사용)
        Path("/System/Library/Fonts/Supplemental/AppleGothic.ttf"),
        # 4. Windows
        Path("C:/Windows/Fonts/malgunbd.ttf"),
        Path("C:/Windows/Fonts/MALGUNBD.TTF"),
    ]

    korean_font = None
    korean_font_bold = None

    # Register regular font
    for font_path in font_paths:
        if font_path.exists():
            try:
                pdfmetrics.registerFont(TTFont("KoreanFont", str(font_path)))
                korean_font = "KoreanFont"
                break
            except Exception:
                continue

    # Register bold font
    for font_path in bold_font_paths:
        if font_path.exists():
            try:
                pdfmetrics.registerFont(TTFont("KoreanFontBold", str(font_path)))
                korean_font_bold = "KoreanFontBold"
                break
            except Exception:
                continue

    # Fallback: use regular font as bold if bold not found
    if not korean_font_bold:
        korean_font_bold = korean_font

    if not korean_font:
        raise RuntimeError(
            f"""한글 폰트를 찾을 수 없습니다.

PDF 생성을 위해:
1. https://hangeul.naver.com/font 에서 나눔고딕 다운로드
2. {fonts_dir} 폴더에 NanumGothic.ttf 파일 복사
3. 애플리케이션 재시작
"""
        )

    # Create PDF in memory
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    # Clean markdown text
    markdown_text = re.sub(r"\[.*?버튼.*?\]", "", markdown_text)
    markdown_text = re.sub(r"\[.*?PDF.*?\]", "", markdown_text)

    # Style definitions
    COLORS = {
        "h1": colors.HexColor("#1a237e"),  # Deep blue
        "h2": colors.HexColor("#303f9f"),  # Indigo
        "h3": colors.HexColor("#3949ab"),  # Lighter indigo
        "h4": colors.HexColor("#5c6bc0"),  # Light indigo
        "text": colors.black,
        "bullet": colors.HexColor("#7986cb"),  # Soft indigo
        "accent": colors.HexColor("#e53935"),  # Red for emphasis
    }

    FONT_SIZES = {
        "h1": 22,
        "h2": 18,
        "h3": 15,
        "h4": 13,
        "body": 11,
        "small": 9,
    }

    LINE_HEIGHTS = {
        "h1": 28,
        "h2": 24,
        "h3": 20,
        "h4": 18,
        "body": 15,
    }

    # Parse and write content
    y_position = height - 1 * inch
    margin_left = 0.75 * inch
    margin_right = width - 0.75 * inch
    max_width = margin_right - margin_left

    lines = markdown_text.split("\n")
    i = 0

    # Chart Render Flag to ensure it is drawn once
    chart_drawn = False

    def new_page():
        nonlocal y_position
        c.showPage()
        y_position = height - 1 * inch

    def draw_text_with_bold(
        text: str,
        x: float,
        y: float,
        font: str,
        font_bold: str,
        size: int,
        color=colors.black,
    ):
        """Draw text with bold sections marked by **text**"""
        c.setFillColor(color)

        # Split by bold markers
        parts = re.split(r"(\*\*.*?\*\*)", text)
        current_x = x

        for part in parts:
            if part.startswith("**") and part.endswith("**"):
                # Bold text
                bold_text = part[2:-2]
                c.setFont(font_bold, size)
                c.drawString(current_x, y, bold_text)
                current_x += c.stringWidth(bold_text, font_bold, size)
            else:
                # Regular text
                c.setFont(font, size)
                c.drawString(current_x, y, part)
                current_x += c.stringWidth(part, font, size)

        c.setFillColor(colors.black)  # Reset color

    def draw_current_chart():
        """Draw all chart images at reduced size (80% width) for better readability"""
        nonlocal y_position, chart_drawn
        if all_charts and not chart_drawn:
            for chart_buf in all_charts:
                try:
                    chart_buf.seek(0)  # Reset buffer position
                    img = ImageReader(chart_buf)
                    img_width, img_height = img.getSize()
                    aspect = img_height / float(img_width)

                    # 80% 너비로 표시하여 컴팩트한 레이아웃
                    display_width = max_width * 0.80
                    display_height = display_width * aspect

                    # 동적 높이 제한 (차트 비율에 따라 조정)
                    # 모든 차트가 너비(80%)를 유지하도록 최대 높이를 페이지 전체로 확장
                    max_chart_height = height - 2 * inch

                    if display_height > max_chart_height:
                        display_height = max_chart_height
                        display_width = display_height / aspect

                    # 페이지 공간 확인
                    if y_position - display_height < 1.5 * inch:
                        new_page()

                    # 중앙 정렬
                    x_offset = margin_left + (max_width - display_width) / 2

                    c.drawImage(
                        img,
                        x_offset,
                        y_position - display_height,
                        width=display_width,
                        height=display_height,
                        mask="auto",
                    )
                    y_position -= display_height + 8  # 차트 간 간격 축소
                except Exception as e:
                    print(f"Chart image render failed: {e}")
            chart_drawn = True

    def draw_heading(text: str, level: int, y_pos: float) -> float:
        """Draw heading with proper styling and word wrap support"""
        nonlocal y_position

        level_map = {1: "h1", 2: "h2", 3: "h3", 4: "h4"}
        style_key = level_map.get(level, "h4")

        font_size = FONT_SIZES[style_key]
        line_height = LINE_HEIGHTS[style_key]
        color = COLORS[style_key]

        # Add extra spacing before heading
        if level <= 2:
            y_pos -= 10

        if y_pos < 1.5 * inch:
            new_page()
            y_pos = y_position

        # Remove markdown heading markers and escape HTML
        clean_text = re.sub(r"^#{1,6}\s+", "", text)
        escaped_text = (
            clean_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )

        # Create paragraph style for heading with word wrap
        heading_style = ParagraphStyle(
            name=f"Heading{level}Style",
            fontName=korean_font_bold,
            fontSize=font_size,
            leading=line_height,
            textColor=color,
            alignment=TA_CENTER if level == 1 else TA_LEFT,
        )

        # Use Paragraph for automatic word wrapping
        p = Paragraph(escaped_text, heading_style)
        p_width, p_height = p.wrap(max_width, height)

        # Check if we need a new page
        if y_pos - p_height < 1 * inch:
            new_page()
            y_pos = y_position

        # Draw underline for H1 and H2 (after paragraph to account for height)
        underline_y = y_pos - p_height - 3
        if level == 1:
            c.setStrokeColor(COLORS["h1"])
            c.setLineWidth(2)
            c.line(margin_left, underline_y, margin_right, underline_y)
        elif level == 2:
            c.setStrokeColor(COLORS["h2"])
            c.setLineWidth(1)
            c.line(margin_left, underline_y, margin_left + 200, underline_y)

        # Draw the heading paragraph
        p.drawOn(c, margin_left, y_pos - p_height)

        return y_pos - p_height - line_height - (8 if level <= 2 else 4)

    while i < len(lines):
        line = lines[i].strip()

        if not line:
            y_position -= 8
            i += 1
            continue

        # Check if this is start of a table
        if "|" in line and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if re.match(r"^\|?[\s\-:|]+\|?[\s\-:|]*$", next_line):
                # Parse table
                table_data = []

                # Add header row
                header_cells = [cell.strip() for cell in line.split("|")]
                header_cells = [c for c in header_cells if c]
                table_data.append(header_cells)

                # Skip separator line
                i += 2

                # Add data rows
                while i < len(lines):
                    row_line = lines[i].strip()
                    if not row_line or "|" not in row_line:
                        break
                    row_cells = [cell.strip() for cell in row_line.split("|")]
                    row_cells = [c for c in row_cells if c]
                    if row_cells:
                        table_data.append(row_cells)
                    i += 1

                # Render table with enhanced styling
                if table_data and len(table_data) > 1:
                    num_cols = len(table_data[0])

                    # 컬럼 수에 따라 폰트 크기 및 패딩 동적 조절
                    if num_cols >= 6:
                        header_font_size = 7
                        data_font_size = 6
                        cell_padding = 4
                    elif num_cols >= 4:
                        header_font_size = 8
                        data_font_size = 7
                        cell_padding = 6
                    else:
                        header_font_size = 10
                        data_font_size = 9
                        cell_padding = 8

                    # 텍스트 자동 줄바꿈을 위해 Paragraph 스타일 설정
                    cell_style = ParagraphStyle(
                        name="TableCellStyle",
                        fontName=korean_font,
                        fontSize=data_font_size,
                        leading=data_font_size + 2,
                        alignment=TA_LEFT,
                        wordWrap="LTR",
                    )
                    header_style = ParagraphStyle(
                        name="TableHeaderStyle",
                        fontName=korean_font_bold,
                        fontSize=header_font_size,
                        leading=header_font_size + 2,
                        alignment=TA_LEFT,
                        textColor=colors.white,
                    )

                    wrapped_data = []
                    for row_idx, row in enumerate(table_data):
                        wrapped_row = []
                        for cell in row:
                            # 1. Escape HTML first
                            clean_cell = (
                                cell.replace("&", "&amp;")
                                .replace("<", "&lt;")
                                .replace(">", "&gt;")
                            )
                            # 2. Apply bold tag with Regex
                            clean_cell = re.sub(
                                r"\*\*(.+?)\*\*", r"<b>\1</b>", clean_cell
                            )

                            if row_idx == 0:
                                wrapped_row.append(Paragraph(clean_cell, header_style))
                            else:
                                wrapped_row.append(Paragraph(clean_cell, cell_style))
                        wrapped_data.append(wrapped_row)

                    # 컬럼 너비 지능형 할당 (마지막 컬럼 '비고'에 더 많은 너비 부여)
                    if num_cols > 2:
                        # 기본 너비를 동일하게 배분하되 비고란이 있다면 비중 조절
                        base_col_width = max_width / (num_cols + 1)
                        col_widths = [base_col_width] * (num_cols - 1)
                        col_widths.append(base_col_width * 2)  # 마지막 컬럼에 2배 할당
                    else:
                        col_widths = [max_width / num_cols] * num_cols

                    table = Table(wrapped_data, colWidths=col_widths)
                    table.setStyle(
                        TableStyle(
                            [
                                # Header styling
                                (
                                    "BACKGROUND",
                                    (0, 0),
                                    (-1, 0),
                                    colors.HexColor("#303f9f"),
                                ),
                                ("VALIGN", (0, 0), (-1, 0), "TOP"),
                                ("BOTTOMPADDING", (0, 0), (-1, 0), cell_padding),
                                ("TOPPADDING", (0, 0), (-1, 0), cell_padding),
                                # Data rows
                                (
                                    "BACKGROUND",
                                    (0, 1),
                                    (-1, -1),
                                    colors.HexColor("#f5f5f5"),
                                ),
                                ("VALIGN", (0, 1), (-1, -1), "TOP"),
                                ("BOTTOMPADDING", (0, 1), (-1, -1), cell_padding),
                                ("TOPPADDING", (0, 1), (-1, -1), cell_padding),
                                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                                # Alternate row colors
                                (
                                    "ROWBACKGROUNDS",
                                    (0, 1),
                                    (-1, -1),
                                    [colors.HexColor("#f5f5f5"), colors.white],
                                ),
                                # Grid and alignment
                                (
                                    "GRID",
                                    (0, 0),
                                    (-1, -1),
                                    0.5,
                                    colors.HexColor("#bdbdbd"),
                                ),
                            ]
                        )
                    )

                    table_width, table_height = table.wrap(max_width, height)

                    if y_position - table_height < 1 * inch:
                        new_page()

                    table.drawOn(c, margin_left, y_position - table_height)
                    y_position -= table_height + 15

                continue

        # Headings
        if line.startswith("#### "):
            y_position = draw_heading(line, 4, y_position)
            i += 1
            continue
        elif line.startswith("### "):
            y_position = draw_heading(line, 3, y_position)
            i += 1
            continue
        elif line.startswith("## "):
            y_position = draw_heading(line, 2, y_position)
            i += 1
            continue
        if line.startswith("# "):
            y_position = draw_heading(line, 1, y_position)

            # 차트 이미지가 있고 아직 그리지 않았다면 제목 다음에 삽입
            if chart_image and not chart_drawn:
                try:
                    img = ImageReader(chart_image)
                    img_width, img_height = img.getSize()
                    aspect = img_height / float(img_width)

                    # 꽉 찬 너비로 설정
                    display_width = max_width
                    display_height = display_width * aspect

                    # 페이지 공간 확인
                    if y_position - display_height < 1 * inch:
                        new_page()

                    c.drawImage(
                        img,
                        margin_left,
                        y_position - display_height,
                        width=display_width,
                        height=display_height,
                        mask="auto",
                    )
                    y_position -= display_height + 20
                    chart_drawn = True
                except Exception as e:
                    print(f"Chart image render failed: {e}")

            i += 1
            continue

        # Horizontal rule
        if line.startswith("---") or line.startswith("***"):
            c.setStrokeColor(colors.HexColor("#e0e0e0"))
            c.setLineWidth(1)
            c.line(margin_left, y_position, margin_right, y_position)
            y_position -= 15
            i += 1
            continue

        if line.startswith("- ") or line.startswith("* "):
            text = line[2:]
            font_size = FONT_SIZES["body"]

            # Escape HTML-like tags for Paragraph and convert bold markers
            escaped_text = (
                text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            )
            bold_text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped_text)

            bullet_style = ParagraphStyle(
                name="BulletStyle",
                fontName=korean_font,
                fontSize=font_size,
                leading=LINE_HEIGHTS["body"],
                leftIndent=0,
                firstLineIndent=0,
            )

            p = Paragraph(bold_text, bullet_style)
            p_width = max_width - 20  # Bullet margin
            p_w, p_h = p.wrap(p_width, height)

            if y_position - p_h < 1 * inch:
                new_page()

            # Draw bullet
            c.setFillColor(COLORS["bullet"])
            c.circle(margin_left + 5, y_position + 1, 3, fill=1)
            c.setFillColor(colors.black)

            # Draw paragraph
            p.drawOn(c, margin_left + 15, y_position - p_h + 8)
            y_position -= p_h + 4
            i += 1
            continue

        if re.match(r"^\d+\.\s", line):
            match = re.match(r"^(\d+)\.\s(.+)$", line)
            if match:
                number = match.group(1)
                text = match.group(2)
                font_size = FONT_SIZES["body"]

                # Escape and convert bold
                escaped_text = (
                    text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                )
                bold_text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped_text)

                num_style = ParagraphStyle(
                    name="NumberStyle",
                    fontName=korean_font,
                    fontSize=font_size,
                    leading=LINE_HEIGHTS["body"],
                )

                p = Paragraph(bold_text, num_style)
                p_width = max_width - 25
                p_w, p_h = p.wrap(p_width, height)

                if y_position - p_h < 1 * inch:
                    new_page()

                # Draw number
                c.setFont(korean_font_bold, font_size)
                c.setFillColor(COLORS["h3"])
                c.drawString(margin_left, y_position, f"{number}.")
                c.setFillColor(colors.black)

                # Draw paragraph
                p.drawOn(c, margin_left + 20, y_position - p_h + 10)
                y_position -= p_h + 4
                i += 1
                continue

        # Regular text with word wrap and bold support
        text = line
        text = re.sub(r"`(.+?)`", r"\1", text)  # Remove code markers
        text = re.sub(r"\[(.+?)\]\(.+?\)", r"\1", text)  # Remove links

        font_size = FONT_SIZES["body"]

        # Escape and convert bold
        escaped_text = (
            text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        bold_text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped_text)

        text_style = ParagraphStyle(
            name="TextStyle",
            fontName=korean_font,
            fontSize=font_size,
            leading=LINE_HEIGHTS["body"],
        )

        p = Paragraph(bold_text, text_style)
        p_w, p_h = p.wrap(max_width, height)

        if y_position - p_h < 1 * inch:
            new_page()

        p.drawOn(c, margin_left, y_position - p_h + 10)
        y_position -= p_h + 2
        i += 1

    # 차트 섹션을 맨 마지막에 렌더링 (텍스트 콘텐츠 이후)
    if all_charts and not chart_drawn:
        # 차트 섹션 헤더 추가
        y_position -= 20  # 여백

        if y_position < 2.5 * inch:
            new_page()

        # 구분선
        c.setStrokeColor(colors.HexColor("#1a237e"))
        c.setLineWidth(1.5)
        c.line(margin_left, y_position, margin_right, y_position)
        y_position -= 15

        # 차트 섹션 제목
        c.setFont(korean_font_bold, FONT_SIZES["h2"])
        c.setFillColor(COLORS["h2"])
        c.drawString(margin_left, y_position, "📊 차트 분석")
        c.setFillColor(colors.black)
        y_position -= 25

        # 차트 렌더링
        draw_current_chart()

    c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes
